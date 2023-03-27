package com.softwaremill.kmq.redelivery

import java.time.Duration
import java.util.Collections
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.std.{Dispatcher, Queue}
import com.softwaremill.kmq.{KmqConfig, MarkerKey, MarkerValue}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import cats.syntax.all._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import fs2.Stream

import scala.jdk.CollectionConverters._
object ConsumeMarkersActor {

  private val logger = Slf4jLogger.getLogger[IO]

  private val OneSecond = Duration.ofSeconds(1)

  def create(config: KmqConfig, helpers: KafkaClientsResourceHelpers): Resource[IO, ConsumeMarkersActor] = {

    def setupMarkerConsumer(
        markerConsumer: KafkaConsumer[MarkerKey, MarkerValue],
        mailbox: Queue[IO, ConsumeMarkersActorMessage],
        dispatcher: Dispatcher[IO]
    ): Resource[IO, Unit] = Resource.eval(
      IO(
        markerConsumer.subscribe(
          Collections.singleton(config.getMarkerTopic),
          new ConsumerRebalanceListener() {
            def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit =
              dispatcher.unsafeRunSync(mailbox.offer(PartitionRevoked(partitions.asScala.toList)))

            def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = ()
          }
        )
      )
    )

    def setupOffsetCommitting = for {
      commitMarkerOffsetsActor <- CommitMarkerOffsetsActor
        .create(
          config.getMarkerTopic,
          config.getMarkerConsumerOffsetGroupId,
          helpers
        )
      _ <- Resource.eval(commitMarkerOffsetsActor.tell(DoCommit))
    } yield commitMarkerOffsetsActor

    for {
      producer <- helpers.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])
      dispatcher <- Dispatcher.sequential[IO]
      mailbox <- Resource.eval(Queue.unbounded[IO, ConsumeMarkersActorMessage])
      markerConsumer <- helpers.createConsumer(
        config.getMarkerConsumerGroupId,
        classOf[MarkerKey.MarkerKeyDeserializer],
        classOf[MarkerValue.MarkerValueDeserializer]
      )
      _ <- setupMarkerConsumer(markerConsumer, mailbox, dispatcher)
      commitMarkerOffsetsActor <- setupOffsetCommitting
      _ <- stream(
        mailbox,
        commitMarkerOffsetsActor,
        markerConsumer,
        dispatcher,
        producer,
        config,
        helpers
      ).compile.drain.background
      _ <- Resource.eval(logger.info("Consume markers actor setup complete"))
    } yield new ConsumeMarkersActor(mailbox)
  }

  private def stream(
      mailbox: Queue[IO, ConsumeMarkersActorMessage],
      commitMarkerOffsetsActor: CommitMarkerOffsetsActor,
      markerConsumer: KafkaConsumer[MarkerKey, MarkerValue],
      dispatcher: Dispatcher[IO],
      producer: KafkaProducer[Array[Byte], Array[Byte]],
      config: KmqConfig,
      helpers: KafkaClientsResourceHelpers
  ) = {

    def partitionAssigned(
        producer: KafkaProducer[Array[Byte], Array[Byte]],
        p: Partition,
        endOffset: Offset,
        dispatcher: Dispatcher[IO]
    ) = for {
      (redeliverActor, shutdown) <- RedeliverActor.create(p, producer, config, helpers.clients).allocated
      _ <- redeliverActor.tell(DoRedeliver)
    } yield AssignedPartition(new MarkersQueue(endOffset - 1), redeliverActor, shutdown, config, None, None, dispatcher)

    def handleRecords(
        assignedPartition: AssignedPartition,
        now: Long,
        partition: Partition,
        records: Iterable[ConsumerRecord[MarkerKey, MarkerValue]]
    ) =
      IO(assignedPartition.handleRecords(records, now)) >>
        assignedPartition.markersQueue
          .smallestMarkerOffset()
          .traverse { offset =>
            commitMarkerOffsetsActor.tell(CommitOffset(partition, offset))
          }
          .void

    val receive: (Map[Partition, AssignedPartition], ConsumeMarkersActorMessage) => IO[
      (Map[Partition, AssignedPartition], Unit)
    ] = {
      case (assignedPartitions, PartitionRevoked(partitions)) =>
        val revokedPartitions = partitions.map(_.partition())

        logger.info(s"Revoked marker partitions: $revokedPartitions") >>
          revokedPartitions
            .flatMap(assignedPartitions.get)
            .traverseTap(_.shutdown)
            .as((assignedPartitions.removedAll(revokedPartitions), ()))
      case (assignedPartitions, DoConsume) =>
        val process = for {
          markers <- IO.blocking(markerConsumer.poll(OneSecond).asScala)
          now <- IO.realTime.map(_.toMillis)
          newlyAssignedPartitions <- markers.groupBy(_.partition()).toList.flatTraverse { case (partition, records) =>
            assignedPartitions.get(partition) match {
              case None =>
                for {
                  endOffsets <- IO(
                    markerConsumer
                      .endOffsets(Collections.singleton(new TopicPartition(config.getMarkerTopic, partition)))
                  )
                  _ <- logger.info(s"Assigned marker partition: $partition")
                  ap <- partitionAssigned(
                    producer,
                    partition,
                    endOffsets.get(partition) - 1,
                    dispatcher
                  )
                  _ <- handleRecords(ap, now, partition, records)
                } yield List((partition, ap))
              case Some(ap) => handleRecords(ap, now, partition, records).as(Nil)
            }
          }
          updatedAssignedPartitions = assignedPartitions ++ newlyAssignedPartitions.toMap
          _ <- updatedAssignedPartitions.values.toList.traverse(_.sendRedeliverMarkers(now))
        } yield updatedAssignedPartitions

        process.guarantee(mailbox.offer(DoConsume)).map((_, ()))
    }

    Stream
      .fromQueueUnterminated(mailbox)
      .evalMapAccumulate(Map.empty[Partition, AssignedPartition])(receive)
      .onFinalize(logger.info("Stopped consume markers actor"))
  }

}
class ConsumeMarkersActor private (
    mailbox: Queue[IO, ConsumeMarkersActorMessage]
) {
  def tell(message: ConsumeMarkersActorMessage): IO[Unit] = mailbox.offer(message)

}

private case class AssignedPartition(
    markersQueue: MarkersQueue,
    redeliverActor: RedeliverActor,
    shutdown: IO[Unit],
    config: KmqConfig,
    var latestSeenMarkerTimestamp: Option[Timestamp],
    var latestMarkerSeenAt: Option[Timestamp],
    dispatcher: Dispatcher[IO]
) {

  private def updateLatestSeenMarkerTimestamp(markerTimestamp: Timestamp, now: Timestamp): Unit = {
    latestSeenMarkerTimestamp = Some(markerTimestamp)
    latestMarkerSeenAt = Some(now)
  }

  def handleRecords(records: Iterable[ConsumerRecord[MarkerKey, MarkerValue]], now: Timestamp): Unit = {
    records.toVector.foreach { record =>
      markersQueue.handleMarker(record.offset(), record.key(), record.value(), record.timestamp())
    }

    updateLatestSeenMarkerTimestamp(records.maxBy(_.timestamp()).timestamp(), now)
  }

  def sendRedeliverMarkers(now: Timestamp): IO[Unit] =
    redeliverTimestamp(now).traverse { rt =>
      for {
        toRedeliver <- IO(markersQueue.markersToRedeliver(rt))
        _ <- redeliverActor.tell(RedeliverMarkers(toRedeliver)).whenA(toRedeliver.nonEmpty)
      } yield ()
    }.void

  private def redeliverTimestamp(now: Timestamp): Option[Timestamp] = {
    // No markers seen at all -> no sense to check for redelivery
    latestMarkerSeenAt.flatMap { lm =>
      if (now - lm < config.getUseNowForRedeliverDespiteNoMarkerSeenForMs) {
        /* If we've seen a marker recently, then using the latest seen marker (which is the maximum marker offset seen
        at all) for computing redelivery. This guarantees that we won't redeliver a message for which an end marker
        was sent, but is waiting in the topic for being observed, even though comparing the wall clock time and start
        marker timestamp exceeds the message timeout. */
        latestSeenMarkerTimestamp
      } else {
        /* If we haven't seen a marker recently, assuming that it's because all available have been observed. Hence
        there are no delays in processing of the markers, so we can use the current time for computing which messages
        should be redelivered. */
        Some(now)
      }
    }
  }
}

sealed trait ConsumeMarkersActorMessage
case object DoConsume extends ConsumeMarkersActorMessage

case class PartitionRevoked(partitions: List[TopicPartition]) extends ConsumeMarkersActorMessage

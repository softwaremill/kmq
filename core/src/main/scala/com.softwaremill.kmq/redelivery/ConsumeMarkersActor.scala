package com.softwaremill.kmq.redelivery

import java.time.Duration
import java.util.Collections

import akka.actor.{Actor, ActorRef, Props}
import com.softwaremill.kmq.{KafkaClients, KmqConfig, MarkerKey, MarkerValue}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConverters._

class ConsumeMarkersActor(clients: KafkaClients, config: KmqConfig) extends Actor with StrictLogging {

  private val OneSecond = Duration.ofSeconds(1)

  private var markerConsumer: KafkaConsumer[MarkerKey, MarkerValue] = _
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  private var assignedPartitions: Map[Partition, AssignedPartition] = Map()

  private var redeliverActorNameCounter = 1

  private var commitMarkerOffsetsActor: ActorRef = _

  override def preStart(): Unit = {
    markerConsumer = clients.createConsumer(config.getRedeliveryConsumerGroupId,
        classOf[MarkerKey.MarkerKeyDeserializer],
        classOf[MarkerValue.MarkerValueDeserializer])
    producer = clients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])

    setupMarkerConsumer()
    setupOffsetCommitting()

    logger.info("Consume markers actor setup complete")
  }

  private def setupMarkerConsumer(): Unit = {
    markerConsumer.subscribe(Collections.singleton(config.getMarkerTopic), new ConsumerRebalanceListener() {
      def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]) {
        logger.info(s"Revoked marker partitions: ${partitions.asScala.toList.map(_.partition())}")
        partitions.asScala.foreach(tp => partitionRevoked(tp.partition()))
      }

      def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]) {
        logger.info(s"Assigned marker partitions: ${partitions.asScala.toList.map(_.partition())}")
        val endOffsets = markerConsumer.endOffsets(partitions)
        partitions.asScala.foreach(tp => partitionAssigned(tp.partition(), endOffsets.get(tp) - 1))
      }
    })
  }

  private def partitionRevoked(p: Partition): Unit = {
    assignedPartitions.get(p).foreach { ap =>
      context.stop(ap.redeliverActor)
    }
    assignedPartitions -= p
  }

  private def partitionAssigned(p: Partition, endOffset: Offset): Unit = {
    val redeliverActorProps =  Props(
      new RedeliverActor(p, new RetryingRedeliverer(new DefaultRedeliverer(p, producer, config, clients))))
      .withDispatcher("kmq.redeliver-dispatcher")
    val redeliverActor = context.actorOf(
      redeliverActorProps,
      s"redeliver-actor-$p-$redeliverActorNameCounter")
    redeliverActor ! DoRedeliver

    assignedPartitions += p -> AssignedPartition(new MarkersQueue(endOffset - 1), redeliverActor, None, None)
    redeliverActorNameCounter += 1
  }

  private def setupOffsetCommitting(): Unit = {
    commitMarkerOffsetsActor = context.actorOf(
      Props(new CommitMarkerOffsetsActor(config.getMarkerTopic, clients)),
      "commit-marker-offsets")

    commitMarkerOffsetsActor ! DoCommit
  }

  override def postStop(): Unit = {
    try markerConsumer.close()
    catch {
      case e: Exception => logger.error("Cannot close marker consumer", e)
    }

    try producer.close()
    catch {
      case e: Exception => logger.error("Cannot close producer", e)
    }

    logger.info("Stopped consume markers actor")
  }

  override def receive: Receive = {
    case DoConsume =>
      try {
        val markers = markerConsumer.poll(OneSecond).asScala
        val now = System.currentTimeMillis()

        markers.groupBy(_.partition()).foreach { case (partition, records) =>
          assignedPartitions.get(partition) match {
            case None =>
              throw new IllegalStateException(s"Got marker for partition $partition: ${records.map(_.key())}, but partition is not assigned!")

            case Some(ap) =>
              ap.handleRecords(records, now)
              ap.markersQueue.smallestMarkerOffset().foreach { offset =>
                commitMarkerOffsetsActor ! CommitOffset(partition, offset)
              }
          }
        }

        assignedPartitions.values.foreach(_.sendRedeliverMarkers(now))
      } finally self ! DoConsume
  }

  private case class AssignedPartition(
    markersQueue: MarkersQueue, redeliverActor: ActorRef,
    var latestSeenMarkerTimestamp: Option[Timestamp], var latestMarkerSeenAt: Option[Timestamp]) {

    def updateLatestSeenMarkerTimestamp(markerTimestamp: Timestamp, now: Timestamp): Unit = {
      latestSeenMarkerTimestamp = Some(markerTimestamp)
      latestMarkerSeenAt = Some(now)
    }

    def handleRecords(records: Iterable[ConsumerRecord[MarkerKey, MarkerValue]], now: Timestamp): Unit = {
      records.foreach { record =>
        markersQueue.handleMarker(record.offset(), record.key(), record.value(), record.timestamp())
      }

      updateLatestSeenMarkerTimestamp(records.maxBy(_.timestamp()).timestamp(), now)
    }

    def sendRedeliverMarkers(now: Timestamp): Unit = {
      redeliverTimestamp(now).foreach { rt =>
        val toRedeliver = markersQueue.markersToRedeliver(rt)
        if (toRedeliver.nonEmpty) {
          redeliverActor ! RedeliverMarkers(toRedeliver)
        }
      }
    }

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
}

case class CommitOffset(p: Partition, o: Offset)
case object DoCommit

case class RedeliverMarkers(markers: List[MarkerKey])
case object DoRedeliver

case object DoConsume

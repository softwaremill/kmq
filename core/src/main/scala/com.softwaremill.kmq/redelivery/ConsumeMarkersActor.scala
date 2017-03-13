package com.softwaremill.kmq.redelivery

import java.util.Collections

import akka.actor.{Actor, ActorRef, Props}
import com.softwaremill.kmq.{KafkaClients, KmqConfig, MarkerKey, MarkerValue}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConverters._

class ConsumeMarkersActor(clients: KafkaClients, config: KmqConfig) extends Actor with StrictLogging {

  private val OneSecond = 1000L

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

    markerConsumer.subscribe(Collections.singleton(config.getMarkerTopic), new ConsumerRebalanceListener() {
      def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]) {
        logger.info(s"Revoked marker partitions: ${partitions.asScala.toList.map(_.partition())}")

        partitions.asScala.foreach { tp =>
          assignedPartitions.get(tp.partition()).foreach { ap =>
            context.stop(ap.redeliverActor)
          }
          assignedPartitions -= tp.partition()
        }
      }

      def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]) {
        logger.info(s"Assigned marker partitions: ${partitions.asScala.toList.map(_.partition())}")

        val endOffsets = markerConsumer.endOffsets(partitions)
        partitions.asScala.foreach { tp =>
          val redeliverActor = context.actorOf(
            Props(new RedeliverActor(tp.partition(),
              new RetryingRedeliverer(new DefaultRedeliverer(tp.partition(), producer, config, clients)))),
            s"redeliver-actor-${tp.partition()}-$redeliverActorNameCounter")
          redeliverActor ! DoRedeliver

          assignedPartitions += tp.partition() -> AssignedPartition(
            new MarkersQueue(endOffsets.get(tp) - 1), redeliverActor, None, None)

          redeliverActorNameCounter += 1
        }
      }
    })

    setupOffsetCommitting()

    logger.info("Consume markers actor setup complete, waiting for the start message to be processed ...")
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
                records.foreach { record =>
                  ap.markersQueue.handleMarker(record.offset(), record.key(), record.value(), record.timestamp())
                }

                ap.updateLatestSeenMarkerTimestamp(records.maxBy(_.timestamp()).timestamp(), now)

                ap.markersQueue.smallestMarkerOffset().foreach { offset =>
                  commitMarkerOffsetsActor ! CommitOffset(partition, offset)
                }
            }
        }

        assignedPartitions.values.foreach { ap =>
          ap.redeliverTimestamp(now).foreach { rt =>
            sendRedeliverMarkers(ap, rt)
          }
        }
      } finally self ! DoConsume
  }

  private case class AssignedPartition(markersQueue: MarkersQueue, redeliverActor: ActorRef,
    var latestSeenMarkerTimestamp: Option[Timestamp], var latestMarkerSeenAt: Option[Timestamp]) {

    def updateLatestSeenMarkerTimestamp(markerTimestmap: Timestamp, now: Timestamp): Unit = {
      latestSeenMarkerTimestamp = Some(markerTimestmap)
      latestMarkerSeenAt = Some(now)
    }

    def redeliverTimestamp(now: Timestamp): Option[Timestamp] = {
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

  private def sendRedeliverMarkers(ap: AssignedPartition, t: Timestamp): Unit = {
    val toRedeliver = ap.markersQueue.markersToRedeliver(t)
    if (toRedeliver.nonEmpty) {
      ap.redeliverActor ! RedeliverMarkers(toRedeliver)
    }
  }
}

case class CommitOffset(p: Partition, o: Offset)
case object DoCommit

case class RedeliverMarkers(markers: List[MarkerKey])
case object DoRedeliver

case object DoConsume
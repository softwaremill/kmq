package com.softwaremill.kmq.redelivery

import java.time.Clock
import java.util.Collections

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props}
import com.softwaremill.kmq.{KafkaClients, KmqConfig, MarkerKey, MarkerValue}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class ConsumeMarkersActor(clients: KafkaClients, config: KmqConfig) extends Actor with StrictLogging {

  private val markersQueues = new MarkersQueues(Clock.systemDefaultZone)

  private var markerConsumer: KafkaConsumer[MarkerKey, MarkerValue] = _
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  private var redeliverActors: Map[Partition, ActorRef] = Map()

  private var redeliverActorNameCounter = 1

  private var commitMarkerOffsetsActor: ActorRef = _

  private var scheduledConsumerMarkers: Cancellable = _

  override def preStart(): Unit = {
    markerConsumer = clients.createConsumer(config.getRedeliveryConsumerGroupId,
      classOf[MarkerKey.MarkerKeyDeserializer],
      classOf[MarkerValue.MarkerValueDeserializer])

    producer = clients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])

    markerConsumer.subscribe(Collections.singleton(config.getMarkerTopic), new ConsumerRebalanceListener() {
      def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]) {
        logger.info(s"Revoked marker partitions: ${partitions.asScala.toList.map(_.partition())}")

        partitions.asScala.foreach { tp =>
          markersQueues.removePartition(tp.partition())
          redeliverActors.get(tp.partition()).foreach { redeliverActor =>
            redeliverActor ! PoisonPill
            redeliverActors -= tp.partition()
          }
        }
      }

      def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]) {
        logger.info(s"Assigned marker partitions: ${partitions.asScala.toList.map(_.partition())}")

        val endOffsets = markerConsumer.endOffsets(partitions)
        partitions.asScala.foreach { tp =>
          markersQueues.addPartition(tp.partition(), endOffsets.get(tp) - 1)
          redeliverActors += tp.partition() -> context.actorOf(
            Props(new RedeliverActor(tp.partition(), new Redeliverer(tp.partition(), producer, config, clients), self)),
            s"redeliver-actor-${tp.partition()}-$redeliverActorNameCounter")

          redeliverActorNameCounter += 1
        }
      }
    })

    setupOffsetCommitting()

    scheduledConsumerMarkers = scheduleConsumeMarkers()

    logger.info("Started consume markers actor")
  }

  private def setupOffsetCommitting(): Unit = {
    commitMarkerOffsetsActor = context.actorOf(
      Props(new CommitMarkerOffsetsActor(config.getMarkerTopic, clients, self)),
      "commit-marker-offsets")

    self ! GetOffsetsToCommit
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

    scheduledConsumerMarkers.cancel()

    logger.info("Stopped consume markers actor")
  }

  override def receive: Receive = {
    case GetOffsetsToCommit =>
      commitMarkerOffsetsActor ! OffsetsToCommit(markersQueues.smallestMarkerOffsetsPerPartition())

    case GetMarkersToRedeliver(partition) =>
      val m = markersQueues.markersToRedeliver(partition)
      if (m.nonEmpty) {
        // not using sender() - the actor might have changed due to rebalancing or restart
        redeliverActors.get(partition).foreach(_ ! MarkersToRedeliver(m, 1))
      }

    case ConsumeMarkers =>
      val markers = markerConsumer.poll(1000L)
      for (record <- markers.asScala) {
        markersQueues.handleMarker(record.offset(), record.key(), record.value())
      }
  }

  private def scheduleConsumeMarkers(): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(1.second, 1.second, self, ConsumeMarkers)
  }
}

case object GetOffsetsToCommit
case class OffsetsToCommit(offsets: Map[Partition, Offset])

case class GetMarkersToRedeliver(partition: Partition)
case class MarkersToRedeliver(markers: List[Marker], retryCounter: Int)

case object ConsumeMarkers
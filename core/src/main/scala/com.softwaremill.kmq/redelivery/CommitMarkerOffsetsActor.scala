package com.softwaremill.kmq.redelivery

import akka.actor.{Actor, ActorRef}
import com.softwaremill.kmq.KafkaClients
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CommitMarkerOffsetsActor(markerTopic: String, clients: KafkaClients, markersActor: ActorRef)
  extends Actor with StrictLogging {

  import context.dispatcher

  private val consumer = clients.createConsumer(null, classOf[ByteArrayDeserializer], classOf[ByteArrayDeserializer])

  override def preStart(): Unit = {
    logger.info("Started commit marker offsets actor")
  }

  override def postStop(): Unit = {
    try consumer.close()
    catch {
      case e: Exception => logger.error("Cannot close commit offsets consumer", e)
    }

    logger.info("Stopped commit marker offsets actor")
  }

  override def receive: Receive = {
    case OffsetsToCommit(m) =>
      /* Committing offsets, and after a second scheduling next commit query. Sending the query first to self, and only
      later forwarding to the main actor. That way, if the actors are being restarted while the schedule is in progress,
      the message will be delivered to a terminated actor and hence dropped (as it should be - another actor will be
      created to replaced this one). If the message was sent straight to `markersActor`, it would be possible that the
      schedule would complete after the main actor restarted and processed the start message, causing duplicate
      commits. */
      try commitOffsets(m)
      finally context.system.scheduler.scheduleOnce(1.second, self, GetOffsetsToCommit)

    case GetOffsetsToCommit =>
      markersActor ! GetOffsetsToCommit
  }

  private def commitOffsets(m: Map[Partition, Offset]): Unit = {
    consumer.commitSync(m.map { case (partition, offset) =>
      (new TopicPartition(markerTopic, partition), new OffsetAndMetadata(offset))
    }.asJava)

    logger.debug(s"Committed marker offsets: $m")
  }
}


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
      try commitOffsets(m)
      finally context.system.scheduler.scheduleOnce(1.second, markersActor, GetOffsetsToCommit)
  }

  private def commitOffsets(m: Map[Partition, Offset]): Unit = {
    consumer.commitSync(m.map { case (partition, offset) =>
      (new TopicPartition(markerTopic, partition), new OffsetAndMetadata(offset))
    }.asJava)

    logger.debug(s"Committed marker offsets: $m")
  }
}


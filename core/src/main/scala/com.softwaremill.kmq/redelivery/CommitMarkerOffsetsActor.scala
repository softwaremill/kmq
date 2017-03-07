package com.softwaremill.kmq.redelivery

import akka.actor.{Actor, ActorRef, Cancellable}
import com.softwaremill.kmq.KafkaClients
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CommitMarkerOffsetsActor(markerTopic: String, clients: KafkaClients, markersActor: ActorRef)
  extends Actor with StrictLogging {

  private val consumer = clients.createConsumer(null, classOf[ByteArrayDeserializer], classOf[ByteArrayDeserializer])
  private var scheduledGetOffsetsQuery: Cancellable = _

  override def preStart(): Unit = {
    scheduledGetOffsetsQuery = scheduleGetOffsetsQuery()

    logger.info("Started commit marker offsets actor")
  }

  override def postStop(): Unit = {
    scheduledGetOffsetsQuery.cancel()

    try consumer.close()
    catch {
      case e: Exception => logger.error("Cannot close commit offsets consumer", e)
    }

    logger.info("Stopped commit marker offsets actor")
  }

  override def receive: Receive = {
    case OffsetsToCommit(m) =>
      consumer.commitSync(m.map { case (partition, offset) =>
        (new TopicPartition(markerTopic, partition), new OffsetAndMetadata(offset))
      }.asJava)

      logger.debug(s"Committed marker offsets: $m")
  }

  private def scheduleGetOffsetsQuery(): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(1.second, 1.second, markersActor, GetOffsetsToCommit)
  }
}


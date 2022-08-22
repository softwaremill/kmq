package com.softwaremill.kmq.redelivery

import akka.actor.Actor
import com.softwaremill.kmq.KafkaClients
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CommitMarkerOffsetsActor(markerTopic: String, clients: KafkaClients) extends Actor with StrictLogging {

  private val consumer = clients.createConsumer(null, classOf[ByteArrayDeserializer], classOf[ByteArrayDeserializer])

  private var toCommit: Map[Partition, Offset] = Map()

  import context.dispatcher

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
    case CommitOffset(p, o) =>
      // only updating if the current offset is smaller
      if (toCommit.get(p).fold(true)(_ < o))
        toCommit += p -> o

    case DoCommit =>
      try {
        commitOffsets()
        toCommit = Map()
      } finally {
        context.system.scheduler.scheduleOnce(1.second, self, DoCommit)
      }
  }

  private def commitOffsets(): Unit = if (toCommit.nonEmpty) {
    consumer.commitSync(toCommit.map { case (partition, offset) =>
      (new TopicPartition(markerTopic, partition), new OffsetAndMetadata(offset))
    }.asJava)

    logger.debug(s"Committed marker offsets: $toCommit")
  }
}

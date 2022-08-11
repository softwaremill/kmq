package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.softwaremill.kmq._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer

import java.io.Closeable
import java.time.Clock
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

object RedeliveryTracker extends StrictLogging {

  def start()(implicit kafkaClients: KafkaClients, config: KmqConfig): Closeable = {
    implicit val system: ActorSystem = ActorSystem("kmq-redelivery")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val clock: Clock = Clock.systemDefaultZone()
    implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
    implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(config.getBootstrapServers)
      .withGroupId(config.getRedeliveryConsumerGroupId)
      .withProperties(config.getConsumerProps)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PartitionFromMarkerKey].getName)

    val streamControl = new RedeliveryTrackerStream(markerConsumerSettings, config.getMarkerTopic, Int.MaxValue)
      .run()

    logger.info("Started redelivery stream")

    () => Await.result(streamControl.drainAndShutdown().andThen { case _ => system.terminate() }, 1.minute)
  }
}

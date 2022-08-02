package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.softwaremill.kmq.{KafkaClients, KmqConfig, MarkerKey, MarkerValue, ParititionFromMarkerKey}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer

import java.io.Closeable
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

object RedeliveryTracker extends StrictLogging {
  def start(bootstrapServers: String, config: KmqConfig): Closeable = {
    implicit val system: ActorSystem = ActorSystem("kmq-redelivery")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
    implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(config.getRedeliveryConsumerGroupId)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    val streamControl = new RedeliveryAndCommitMarkerStream(markerConsumerSettings,
      config.getMarkerTopic, 64,
      new KafkaClients(bootstrapServers), config)
      .run()

    logger.info("Started redelivery stream")

    () => Await.result(streamControl.drainAndShutdown().andThen { case _ => system.terminate() }, 1.minute)
  }
}

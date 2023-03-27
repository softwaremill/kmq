package com.softwaremill.kmq.redelivery

import cats.effect.{IO, Resource}
import com.softwaremill.kmq.KafkaClients
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.jdk.CollectionConverters._

private[redelivery] final class KafkaClientsResourceHelpers(val clients: KafkaClients) {

  def createProducer[K, V](
      keySerializer: Class[_ <: Serializer[K]],
      valueSerializer: Class[_ <: Serializer[V]],
      extraConfig: Map[String, AnyRef] = Map.empty
  ): Resource[IO, KafkaProducer[K, V]] =
    Resource.make(
      IO(clients.createProducer(keySerializer, valueSerializer, extraConfig.asJava))
    )(p => IO(p.close()))

  def createConsumer[K, V](
      groupId: String,
      keyDeserializer: Class[_ <: Deserializer[K]],
      valueDeserializer: Class[_ <: Deserializer[V]],
      extraConfig: Map[String, AnyRef] = Map.empty
  ): Resource[IO, KafkaConsumer[K, V]] = Resource.make(
    IO(clients.createConsumer(groupId, keyDeserializer, valueDeserializer, extraConfig.asJava))
  )(p => IO(p.close()))

}

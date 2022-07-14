package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.Materializer
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class StartMarkerStreamIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val markerKeySerializer: Serializer[MarkerKey] = new MarkerKey.MarkerKeySerializer()
  implicit val markerValueSerializer: Serializer[MarkerValue] = new MarkerValue.MarkerValueSerializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
  implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

  "StartMarkerStream" should "send StartMarker for each new message" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig = new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000)

    createTopic(kmqConfig.getMsgTopic)
    createTopic(kmqConfig.getMarkerTopic)

    val msgConsumerSettings = ConsumerSettings(system, stringDeserializer, stringDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val markerProducerSettings = ProducerSettings(system, markerKeySerializer, markerValueSerializer)
      .withBootstrapServers(bootstrapServer)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    val startMarkerStreamControl = new StartMarkerStream(msgConsumerSettings, markerProducerSettings,
      kmqConfig.getMsgTopic, kmqConfig.getMarkerTopic, 64, 3.second.toMillis)
      .run()

    (1 to 10)
      .map(_.toString)
      .foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg))

    eventually {
      consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, "other").size shouldBe 10
    }(PatienceConfig(timeout = Span(15, Seconds)), implicitly, implicitly)

    startMarkerStreamControl.drainAndShutdown()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

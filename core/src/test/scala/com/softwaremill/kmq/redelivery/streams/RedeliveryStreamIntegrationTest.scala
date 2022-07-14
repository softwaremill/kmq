package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.Materializer
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.{Seconds, Span}

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class RedeliveryStreamIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val markerKeySerializer: Serializer[MarkerKey] = new MarkerKey.MarkerKeySerializer()
  implicit val markerValueSerializer: Serializer[MarkerValue] = new MarkerValue.MarkerValueSerializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
  implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

  "RedeliveryStream" should "redeliver unprocessed messages" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val maxRedeliveryCount = 1
    val kmqConfig = new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000, s"${uid}__undelivered", "kmq-redelivery-count", maxRedeliveryCount)

    createTopic(kmqConfig.getMsgTopic)
    createTopic(kmqConfig.getMarkerTopic)

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getRedeliveryConsumerGroupId)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    val redeliveryStreamControl = new RedeliveryStream(markerConsumerSettings,
      kmqConfig.getMarkerTopic, 64,
      new KafkaClients(bootstrapServer), kmqConfig)
      .run()

    (1 to 10).foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg.toString))

    (1 to 10).foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, startMarker(msg)))

    Seq(1, 2, 3, 5).foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, endMarker(msg)))

    eventually {
      consumeAllFromKafkaWithoutCommit[String, String](kmqConfig.getMsgTopic, "other").size shouldBe 16
      consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, "other").size shouldBe 20
    }(PatienceConfig(timeout = Span(15, Seconds)), implicitly, implicitly)

    redeliveryStreamControl.drainAndShutdown()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  def startMarker(msg: Int): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msg) -> new StartMarker(now.toMillis + 100).asInstanceOf[MarkerValue]

  def endMarker(msg: Int): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msg) -> EndMarker.INSTANCE.asInstanceOf[MarkerValue]
}
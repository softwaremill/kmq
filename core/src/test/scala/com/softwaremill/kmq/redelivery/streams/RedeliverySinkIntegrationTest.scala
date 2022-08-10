package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
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

import java.time.Clock
import java.util.UUID
import scala.concurrent.ExecutionContext

class RedeliverySinkIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val markerKeySerializer: Serializer[MarkerKey] = new MarkerKey.MarkerKeySerializer()
  implicit val markerValueSerializer: Serializer[MarkerValue] = new MarkerValue.MarkerValueSerializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
  implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

  "RedeliverySink" should "redeliver unprocessed messages" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val maxRedeliveryCount = 1
    val redeliverAfterMs = 300

    implicit val kafkaClients: KafkaClients = new KafkaClients(bootstrapServer)
    implicit val kmqConfig: KmqConfig = new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000, s"${uid}__undelivered", "kmq-redelivery-count", maxRedeliveryCount)
    implicit val clock: Clock = Clock.systemDefaultZone()

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getRedeliveryConsumerGroupId)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    val streamControl = Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(kmqConfig.getMarkerTopic))
      .map { case (topicPartition, source) =>
        source
          .toMat(RedeliverySink(topicPartition.partition))(Keep.right)
          .run()
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()

    createTopic(kmqConfig.getMsgTopic)
    createTopic(kmqConfig.getMarkerTopic)

    (0 to 9).foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg.toString))

    (0 to 9).foreach(msgOffset => sendToKafka(kmqConfig.getMarkerTopic, startMarker(msgOffset, redeliverAfterMs)))

    Seq(0, 1, 2, 5).foreach(msgOffset => sendToKafka(kmqConfig.getMarkerTopic, endMarker(msgOffset)))

    eventually {
      consumeAllFromKafkaWithoutCommit[String, String](kmqConfig.getMsgTopic, "other").size shouldBe 16
      consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, "other").size shouldBe 20
    }(PatienceConfig(timeout = Span(30, Seconds)), implicitly, implicitly)

    streamControl.drainAndShutdown()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  def startMarker(msgOffset: Int, redeliverAfterMs: Long): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msgOffset) -> new StartMarker(redeliverAfterMs).asInstanceOf[MarkerValue]

  def endMarker(msgOffset: Int): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msgOffset) -> EndMarker.INSTANCE.asInstanceOf[MarkerValue]
}

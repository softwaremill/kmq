package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.Materializer
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.Offset
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.{Seconds, Span}

import java.time.Clock
import java.util.{Collections, UUID}
import scala.concurrent.ExecutionContext

class RedeliveryTrackerStreamIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val clock: Clock = Clock.systemDefaultZone()

  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val markerKeySerializer: Serializer[MarkerKey] = new MarkerKey.MarkerKeySerializer()
  implicit val markerValueSerializer: Serializer[MarkerValue] = new MarkerValue.MarkerValueSerializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
  implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

  "RedeliveryTrackerStream" should "redeliver unprocessed messages" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val maxRedeliveryCount = 1
    val redeliverAfterMs = 300

    implicit val kmqConfig: KmqConfig = new KmqConfig(bootstrapServer, s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000, s"${uid}__undelivered", "kmq-redelivery-count", maxRedeliveryCount, Collections.emptyMap())
    implicit val kafkaClients: KafkaClients = new KafkaClients(kmqConfig)

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getRedeliveryConsumerGroupId)
      .withProperties(kmqConfig.getConsumerProps)

    val streamControl = new RedeliveryTrackerStream(markerConsumerSettings,
      kmqConfig.getMarkerTopic, Int.MaxValue)
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

  "RedeliveryTrackerStream" should "commit all markers before first open StartMarker" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val redeliverAfterMs = 300

    implicit val kmqConfig: KmqConfig = new KmqConfig(bootstrapServer, s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000)
    implicit val kafkaClients: KafkaClients = new KafkaClients(kmqConfig)

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getRedeliveryConsumerGroupId)
      .withProperties(kmqConfig.getConsumerProps)

    val streamControl = new RedeliveryTrackerStream(markerConsumerSettings,
      kmqConfig.getMarkerTopic, Int.MaxValue)
      .run()

    createTopic(kmqConfig.getMarkerTopic)

    (1 to 10)
      .foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, startMarker(msg, redeliverAfterMs)))

    Seq(1, 2, 3, 5)
      .foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, endMarker(msg)))

    streamControl.drainAndShutdown().andThen { _ =>
      eventually {
        val markers = consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, "other")
        val uncommittedMarkers = consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, kmqConfig.getRedeliveryConsumerGroupId)

        markers.groupByTypeAndMapToOffset() should contain theSameElementsAs Map(
          "StartMarker" -> (1 to 10),
          "EndMarker" -> Seq(1, 2, 3, 5)
        )

        uncommittedMarkers.groupByTypeAndMapToOffset() should contain theSameElementsAs Map(
          "StartMarker" -> (4 to 10),
          "EndMarker" -> Seq(1, 2, 3, 5)
        )
      }(PatienceConfig(timeout = Span(30, Seconds)), implicitly, implicitly)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  def startMarker(msgOffset: Int, redeliverAfterMs: Long): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msgOffset) -> new StartMarker(redeliverAfterMs).asInstanceOf[MarkerValue]

  def endMarker(msgOffset: Int): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msgOffset) -> EndMarker.INSTANCE.asInstanceOf[MarkerValue]

  implicit class GroupByTypeAndMapToOffsetOperation(markers: Seq[ConsumerRecord[MarkerKey, MarkerValue]]) {
    def groupByTypeAndMapToOffset(): Map[String, Seq[Offset]] = {
      markers.groupMap(_.value.getClass.getSimpleName)(_.key.getMessageOffset)
    }
  }
}

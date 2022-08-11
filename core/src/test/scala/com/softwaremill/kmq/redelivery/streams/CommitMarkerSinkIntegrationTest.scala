package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
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
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}

import java.util.UUID
import scala.concurrent.ExecutionContext

class CommitMarkerSinkIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val markerKeySerializer: Serializer[MarkerKey] = new MarkerKey.MarkerKeySerializer()
  implicit val markerValueSerializer: Serializer[MarkerValue] = new MarkerValue.MarkerValueSerializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val markerKeyDeserializer: Deserializer[MarkerKey] = new MarkerKey.MarkerKeyDeserializer()
  implicit val markerValueDeserializer: Deserializer[MarkerValue] = new MarkerValue.MarkerValueDeserializer()

  "CommitMarkerSink" should "commit all markers before first open StartMarker" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig = new KmqConfig(bootstrapServer, s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000)

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getRedeliveryConsumerGroupId)
      .withProperties(kmqConfig.getConsumerProps)

    val streamControl = Consumer.committableSource(markerConsumerSettings, Subscriptions.topics(kmqConfig.getMarkerTopic))
      .toMat(CommitMarkerSink())(DrainingControl.apply)
      .run()

    createTopic(kmqConfig.getMarkerTopic)

    (1 to 10)
      .foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, startMarker(msg)))

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

  def startMarker(msgOffset: Int): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msgOffset) -> new StartMarker(100).asInstanceOf[MarkerValue]

  def endMarker(msgOffset: Int): (MarkerKey, MarkerValue) =
    new MarkerKey(0, msgOffset) -> EndMarker.INSTANCE.asInstanceOf[MarkerValue]

  implicit class GroupByTypeAndMapToOffsetOperation(markers: Seq[ConsumerRecord[MarkerKey, MarkerValue]]) {
    def groupByTypeAndMapToOffset(): Map[String, Seq[Offset]] = {
      markers.groupMap(_.value.getClass.getSimpleName)(_.key.getMessageOffset)
    }
  }
}

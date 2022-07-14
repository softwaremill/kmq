package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.Materializer
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.Offset
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

class CommitMarkerOffsetsIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

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

    val markerConsumerSettings = ConsumerSettings(system, markerKeyDeserializer, markerValueDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getRedeliveryConsumerGroupId)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    val commitMarkerOffsetsStreamControl = new CommitMarkerOffsetsStream(markerConsumerSettings,
      kmqConfig.getMarkerTopic, 64)
      .run()

    createTopic(kmqConfig.getMarkerTopic)

    (1 to 10)
      .foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, new MarkerKey(0, msg), new StartMarker(now.toMillis + 100).asInstanceOf[MarkerValue]))

    Seq(1, 2, 3, 5)
      .foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, new MarkerKey(0, msg), EndMarker.INSTANCE.asInstanceOf[MarkerValue]))

    Thread.sleep(1.seconds.toMillis) //TODO: await for stream to process all markers

    // wait until stream shutdown, so there is no active consumer left with given groupId
    Await.ready(commitMarkerOffsetsStreamControl.drainAndShutdown(), 60.seconds)

    val markers = consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, "other")
    val uncommittedMarkers = consumeAllFromKafkaWithoutCommit[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, kmqConfig.getRedeliveryConsumerGroupId)

    offsetsByMarkerType(markers) should contain theSameElementsAs Map(
      "StartMarker" -> (1 to 10),
      "EndMarker" -> Seq(1, 2, 3, 5)
    )

    offsetsByMarkerType(uncommittedMarkers) should contain theSameElementsAs Map(
      "StartMarker" -> (4 to 10),
      "EndMarker" -> Seq(1, 2, 3, 5)
    )
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  def offsetsByMarkerType(markers: List[ConsumerRecord[MarkerKey, MarkerValue]]): Map[String, Seq[Offset]] = {
    markers.groupMap(_.value.getClass.getSimpleName)(_.key.getMessageOffset)
  }
}

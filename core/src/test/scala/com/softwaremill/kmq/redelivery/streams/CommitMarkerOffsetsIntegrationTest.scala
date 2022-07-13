package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.Materializer
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

class CommitMarkerOffsetsIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val executionContext: ExecutionContext = system.dispatcher

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

    (1 to 3)
      .foreach(msg => sendToKafka(kmqConfig.getMarkerTopic, new MarkerKey(0, msg), EndMarker.INSTANCE.asInstanceOf[MarkerValue]))

    Thread.sleep(15.seconds.toMillis)
    Await.ready(commitMarkerOffsetsStreamControl.shutdown(), 60.seconds)

    val markers = consumeAllFromKafkaWithoutCommit(kmqConfig.getMarkerTopic, "other")(new MarkerKey.MarkerKeyDeserializer, new MarkerValue.MarkerValueDeserializer)
    val uncommittedMarkers = consumeAllFromKafkaWithoutCommit(kmqConfig.getMarkerTopic, kmqConfig.getRedeliveryConsumerGroupId)(new MarkerKey.MarkerKeyDeserializer, new MarkerValue.MarkerValueDeserializer)

    markers.size shouldBe 13
    uncommittedMarkers.size shouldBe 7
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

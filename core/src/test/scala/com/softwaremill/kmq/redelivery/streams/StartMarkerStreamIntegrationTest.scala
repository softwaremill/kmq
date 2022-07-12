package com.softwaremill.kmq.redelivery.streams

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class StartMarkerStreamIntegrationTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val executionContext: ExecutionContext = system.dispatcher

  "StartMarkerStream" should "send StartMarker for each new message" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig = new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_redelivery",
      1000, 1000)

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val markerProducerSettings = ProducerSettings(system,
      new MarkerKey.MarkerKeySerializer(), new MarkerValue.MarkerValueSerializer())
      .withBootstrapServers(bootstrapServer)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    lazy val markerMessages = ArrayBuffer[String]()

    val startMarkerStreamControl = new StartMarkerStream(consumerSettings, markerProducerSettings,
      kmqConfig.getMsgTopic, kmqConfig.getMarkerTopic, 64, 3.second.toMillis)
      .run()

    (1 to 10)
      .map(_.toString)
      .foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg))

    val markersControl = Consumer.plainSource(consumerSettings, Subscriptions.topics(kmqConfig.getMarkerTopic))
      .map { msg =>
        markerMessages += msg.value
      }
      .to(Sink.ignore)
      .run()

    eventually {
      markerMessages.size shouldBe 10
    }(PatienceConfig(timeout = Span(15, Seconds)), implicitly, implicitly)

    markersControl.shutdown()
    startMarkerStreamControl.drainAndShutdown()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

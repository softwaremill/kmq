package com.softwaremill.kmq.redelivery

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}

import java.util.{Random, UUID}
import scala.collection.mutable.ArrayBuffer

class IntegrationTest
    extends TestKit(ActorSystem("test-system"))
    with AnyFlatSpecLike
    with KafkaSpec
    with BeforeAndAfterAll
    with Eventually {

  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  "KMQ" should "resend message if not committed" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig =
      new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_marker", "kmq_marker_offset", 1000, 1000)

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val markerProducerSettings =
      ProducerSettings(system, new MarkerKey.MarkerKeySerializer(), new MarkerValue.MarkerValueSerializer())
        .withBootstrapServers(bootstrapServer)
        .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    val random = new Random()

    lazy val processedMessages = ArrayBuffer[String]()
    lazy val receivedMessages = ArrayBuffer[String]()

    val control = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(kmqConfig.getMsgTopic)) // 1. get messages from topic
      .map { msg =>
        ProducerMessage.Message(
          new ProducerRecord[MarkerKey, MarkerValue](
            kmqConfig.getMarkerTopic,
            MarkerKey.fromRecord(msg.record),
            new StartMarker(kmqConfig.getMsgTimeoutMs)
          ),
          msg
        )
      }
      .via(Producer.flexiFlow(markerProducerSettings)) // 2. write the "start" marker
      .map(_.passThrough)
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl().map(_ => msg.record) // this should be batched
      }
      .map { msg =>
        receivedMessages += msg.value
        msg
      }
      .filter(_ => random.nextInt(5) != 0)
      .map { processedMessage =>
        processedMessages += processedMessage.value
        new ProducerRecord[MarkerKey, MarkerValue](
          kmqConfig.getMarkerTopic,
          MarkerKey.fromRecord(processedMessage),
          EndMarker.INSTANCE
        )
      }
      .to(Producer.plainSink(markerProducerSettings)) // 5. write "end" markers
      .run()

    val redeliveryHook = RedeliveryTracker.start(new KafkaClients(bootstrapServer), kmqConfig)

    val messages = (0 to 20).map(_.toString)
    messages.foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg))

    eventually {
      receivedMessages.size should be > processedMessages.size
      processedMessages.sortBy(_.toInt).distinct shouldBe messages
    }(PatienceConfig(timeout = Span(30, Seconds)), implicitly, implicitly)

    redeliveryHook.close()
    control.shutdown()
  }

  "KMQ" should "resend message if max redelivery count not exceeded" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig =
      new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_marker", "kmq_marker_offset", 1000, 1000)

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val markerProducerSettings =
      ProducerSettings(system, new MarkerKey.MarkerKeySerializer(), new MarkerValue.MarkerValueSerializer())
        .withBootstrapServers(bootstrapServer)
        .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

    lazy val receivedMessages = ArrayBuffer[String]()
    lazy val undeliveredMessages = ArrayBuffer[String]()

    val control = Consumer
      .committableSource(consumerSettings, Subscriptions.topics(kmqConfig.getMsgTopic)) // 1. get messages from topic
      .map { msg =>
        ProducerMessage.Message(
          new ProducerRecord[MarkerKey, MarkerValue](
            kmqConfig.getMarkerTopic,
            MarkerKey.fromRecord(msg.record),
            new StartMarker(kmqConfig.getMsgTimeoutMs)
          ),
          msg
        )
      }
      .via(Producer.flexiFlow(markerProducerSettings)) // 2. write the "start" marker
      .map(_.passThrough)
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl().map(_ => msg.record) // this should be batched
      }
      .map { msg =>
        receivedMessages += msg.value
        msg
      }
      .filter(msg => msg.value.toInt % 3 != 0)
      .map { processedMessage =>
        new ProducerRecord[MarkerKey, MarkerValue](
          kmqConfig.getMarkerTopic,
          MarkerKey.fromRecord(processedMessage),
          EndMarker.INSTANCE
        )
      }
      .to(Producer.plainSink(markerProducerSettings)) // 5. write "end" markers
      .run()

    val undeliveredControl = Consumer
      .plainSource(
        consumerSettings,
        Subscriptions.topics(s"${kmqConfig.getMsgTopic}__undelivered")
      ) // 1. get messages from dead-letter topic
      .map { msg =>
        undeliveredMessages += msg.value
        msg
      }
      .to(Sink.ignore)
      .run()

    val redeliveryHook = RedeliveryTracker.start(new KafkaClients(bootstrapServer), kmqConfig)

    val messages = (0 to 6).map(_.toString)
    messages.foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg))
    val expectedReceived = Array(0, 0, 0, 0, 1, 2, 3, 3, 3, 3, 4, 5, 6, 6, 6, 6).map(_.toString)
    val expectedUndelivered = Array(0, 3, 6).map(_.toString)

    eventually {
      receivedMessages.sortBy(_.toInt) shouldBe expectedReceived
      undeliveredMessages.sortBy(_.toInt) shouldBe expectedUndelivered
    }(PatienceConfig(timeout = Span(30, Seconds)), implicitly, implicitly)

    redeliveryHook.close()
    control.shutdown()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

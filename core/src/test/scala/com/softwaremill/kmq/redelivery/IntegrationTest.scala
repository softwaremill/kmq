package com.softwaremill.kmq.redelivery

import java.time.Duration
import java.util.Random

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.mutable.ArrayBuffer

class IntegrationTest extends TestKit(ActorSystem("test-system")) with FlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually with Matchers {

  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  "KMQ" should "resend message if not committed" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val kmqConfig = new KmqConfig("queue", "markers", "kmq_client", "kmq_redelivery", Duration.ofSeconds(1).toMillis,
    1000)

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val markerProducerSettings = ProducerSettings(system,
      new MarkerKey.MarkerKeySerializer(), new MarkerValue.MarkerValueSerializer())
      .withBootstrapServers(bootstrapServer)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)
    val markerProducer = markerProducerSettings.createKafkaProducer()

    val random = new Random()

    lazy val processedMessages = ArrayBuffer[String]()
    lazy val receivedMessages = ArrayBuffer[String]()

    val control = Consumer.committableSource(consumerSettings, Subscriptions.topics(kmqConfig.getMsgTopic)) // 1. get messages from topic
      .map { msg =>
      ProducerMessage.Message(
        new ProducerRecord[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, MarkerKey.fromRecord(msg.record), new StartMarker(kmqConfig.getMsgTimeoutMs)), msg)
    }
      .via(Producer.flow(markerProducerSettings, markerProducer)) // 2. write the "start" marker
      .map(_.message.passThrough)
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
        new ProducerRecord[MarkerKey, MarkerValue](kmqConfig.getMarkerTopic, MarkerKey.fromRecord(processedMessage), EndMarker.INSTANCE)
      }
      .to(Producer.plainSink(markerProducerSettings, markerProducer)) // 5. write "end" markers
      .run()

    val redeliveryHook = RedeliveryTracker.start(new KafkaClients(bootstrapServer), kmqConfig)

    val messages = (0 to 20).map(_.toString)
    messages.foreach(msg => sendToKafka(kmqConfig.getMsgTopic,msg))

    eventually {
      receivedMessages.size should be > processedMessages.size
      processedMessages.sortBy(_.toInt).distinct shouldBe messages
    }(PatienceConfig(timeout = Span(15, Seconds)), implicitly)

    redeliveryHook.close()
    control.shutdown()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

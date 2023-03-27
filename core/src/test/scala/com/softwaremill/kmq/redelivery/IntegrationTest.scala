package com.softwaremill.kmq.redelivery

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}

import java.util.{Properties, Random, UUID}
import scala.collection.mutable.ArrayBuffer
import cats.effect.unsafe.implicits.global
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, KafkaConsumer}
import scala.util.chaining._

class IntegrationTest extends AnyFlatSpecLike with KafkaSpec with Eventually {

  "KMQ" should "resend message if not ted" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig =
      new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_marker", "kmq_marker_offset", 1000, 1000)

    val random = new Random()

    lazy val processedMessages = ArrayBuffer[String]()
    lazy val receivedMessages = ArrayBuffer[String]()

    val consumerSettings = ConsumerSettings[IO, Unit, String]
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

    val properties = new Properties()
      .tap(_.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer))
      .tap(_.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[MarkerKey.MarkerKeySerializer].getName))
      .tap(
        _.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[MarkerValue.MarkerValueSerializer].getName)
      )
      .tap(_.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PartitionFromMarkerKey].getName))

    val resources = for {
      consumer <- KafkaConsumer
        .resource(consumerSettings)
        .evalTap(
          _.subscribe(NonEmptyList.one(kmqConfig.getMsgTopic))
        )
      producer <- Resource.make(IO(new KafkaProducer[MarkerKey, MarkerValue](properties)))(p => IO(p.close()))
    } yield (consumer, producer)

    resources
      .flatMap { case (consumer, producer) =>
        consumer // 1. get messages from topic
          .stream
          .evalTap { msg =>
            // 2. write the "start" marker
            IO(
              producer.send(
                new ProducerRecord[MarkerKey, MarkerValue](
                  kmqConfig.getMarkerTopic,
                  new MarkerKey(msg.record.partition, msg.record.offset),
                  new StartMarker(kmqConfig.getMsgTimeoutMs)
                )
              )
            ) >> msg.offset.commit >> IO(receivedMessages += msg.record.value)

          }
          .filter(_ => random.nextInt(5) != 0)
          .evalTap { processedMessage =>
            IO(processedMessages += processedMessage.record.value) >>
              IO(
                producer.send( // 5. write "end" markers
                  new ProducerRecord[MarkerKey, MarkerValue](
                    kmqConfig.getMarkerTopic,
                    new MarkerKey(processedMessage.record.partition, processedMessage.record.offset),
                    new StartMarker(kmqConfig.getMsgTimeoutMs)
                  )
                )
              )
          }
          .compile
          .drain
          .background
      }
      .surround {

        IO(RedeliveryTracker.start(new KafkaClients(bootstrapServer), kmqConfig)).bracket { _ =>
          IO {
            val messages = (0 to 20).map(_.toString)
            messages.foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg))

            eventually {
              receivedMessages.size should be > processedMessages.size
              processedMessages.sortBy(_.toInt).distinct shouldBe messages

            }(PatienceConfig(timeout = Span(30, Seconds)), implicitly, implicitly)
          }
        }(rt => IO(rt.close()))
      }
      .unsafeRunSync()

  }

  "KMQ" should "resend message if max redelivery count not exceeded" in {
    val bootstrapServer = s"localhost:${testKafkaConfig.kafkaPort}"
    val uid = UUID.randomUUID().toString
    val kmqConfig =
      new KmqConfig(s"$uid-queue", s"$uid-markers", "kmq_client", "kmq_marker", "kmq_marker_offset", 1000, 1000)

    lazy val receivedMessages = ArrayBuffer[String]()
    lazy val undeliveredMessages = ArrayBuffer[String]()

    val consumerSettings = ConsumerSettings[IO, Unit, String]
      .withBootstrapServers(bootstrapServer)
      .withGroupId(kmqConfig.getMsgConsumerGroupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

    val properties = new Properties()
      .tap(_.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer))
      .tap(_.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[MarkerKey.MarkerKeySerializer].getName))
      .tap(
        _.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[MarkerValue.MarkerValueSerializer].getName)
      )
      .tap(_.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PartitionFromMarkerKey].getName))

    val resources = for {
      consumer <- KafkaConsumer
        .resource(consumerSettings)
        .evalTap(
          _.subscribe(NonEmptyList.one(kmqConfig.getMsgTopic))
        )
      producer <- Resource.make(IO(new KafkaProducer[MarkerKey, MarkerValue](properties)))(p => IO(p.close()))
    } yield (consumer, producer)

    resources
      .flatMap { case (consumer, producer) =>
        consumer.stream
          .evalTap { msg =>
            IO(
              producer.send(
                new ProducerRecord[MarkerKey, MarkerValue](
                  kmqConfig.getMarkerTopic,
                  new MarkerKey(msg.record.partition, msg.record.offset),
                  new StartMarker(kmqConfig.getMsgTimeoutMs)
                )
              )
            ) >> msg.offset.commit >> IO(receivedMessages += msg.record.value)
          }
          .filter(_.record.value.toInt % 3 != 0)
          .evalTap(msg =>
            IO(
              producer.send(
                new ProducerRecord[MarkerKey, MarkerValue](
                  kmqConfig.getMarkerTopic,
                  new MarkerKey(msg.record.partition, msg.record.offset),
                  EndMarker.INSTANCE
                )
              )
            )
          )
          .compile
          .drain
          .background
      }
      .flatMap(_ =>
        KafkaConsumer
          .resource(consumerSettings)
          .evalTap( // 1. get messages from dead-letter topic
            _.subscribe(NonEmptyList.one(s"${kmqConfig.getMsgTopic}__undelivered"))
          )
          .flatMap(consumer =>
            consumer.stream
              .evalTap(msg => IO(undeliveredMessages += msg.record.value))
              .compile
              .drain
              .background
          )
      )
      .surround {

        IO(RedeliveryTracker.start(new KafkaClients(bootstrapServer), kmqConfig)).bracket { _ =>
          IO {
            val messages = (0 to 6).map(_.toString)
            messages.foreach(msg => sendToKafka(kmqConfig.getMsgTopic, msg))
            val expectedReceived = Array(0, 0, 0, 0, 1, 2, 3, 3, 3, 3, 4, 5, 6, 6, 6, 6).map(_.toString)
            val expectedUndelivered = Array(0, 3, 6).map(_.toString)

            eventually {
              receivedMessages.sortBy(_.toInt) shouldBe expectedReceived
              undeliveredMessages.sortBy(_.toInt) shouldBe expectedUndelivered
            }(PatienceConfig(timeout = Span(30, Seconds)), implicitly, implicitly)
          }
        }(rt => IO(rt.close()))
      }
      .unsafeRunSync()

  }

}

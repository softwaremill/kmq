package com.softwaremill.kmq.redelivery.infrastructure

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig, EmbeddedKafkaConfigImpl, duration2JavaDuration}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer}
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.duration.{DurationInt, SECONDS}
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsJava, SeqHasAsJava}
import scala.util.{Failure, Try}

trait KafkaSpec extends BeforeAndAfterEach {
  self: Suite =>

  val testKafkaConfig: EmbeddedKafkaConfigImpl = EmbeddedKafkaConfig(9092, 2182).asInstanceOf[EmbeddedKafkaConfigImpl]
  private implicit val stringDeserializer: StringDeserializer = new StringDeserializer()

  def createTopic(topic: String): Unit = {
    EmbeddedKafka.createCustomTopic(topic)
  }

  def sendToKafka(topic: String, message: String): Unit = {
    EmbeddedKafka.publishStringMessageToKafka(topic, message)(testKafkaConfig)
  }

  def sendToKafka[K, V](topic: String, message: (K, V))
                       (implicit keySerializer: Serializer[K], valueSerializer: Serializer[V]): Unit = {
    val props = Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${testKafkaConfig.kafkaPort}",
      ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    ).asJava

    val producer = new KafkaProducer[K, V](props, keySerializer, valueSerializer)
    val sendFuture = producer.send(new ProducerRecord(topic, message._1, message._2))
    val sendResult = Try {
      sendFuture.get(10, SECONDS)
    }

    producer.close()

    sendResult match {
      case Failure(ex) => throw new RuntimeException(ex)
      case _ => // OK
    }
  }

  def consumeFromKafka(topic: String): String = {
    EmbeddedKafka.consumeFirstStringMessageFrom(topic)(testKafkaConfig)
  }

  def consumeAllFromKafkaWithoutCommit[K, V](topic: String, consumerGroup: String)
                                            (implicit keyDeserializer: Deserializer[K],
                                             valueDeserializer: Deserializer[V]): List[ConsumerRecord[K, V]] = {
    val props = Map[String, Object](
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${testKafkaConfig.kafkaPort}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OffsetResetStrategy.EARLIEST.toString.toLowerCase,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> false.toString
    ).asJava

    val consumer = new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)
    consumer.subscribe(Seq(topic).asJava)
    val records = consumer.poll(duration2JavaDuration(10.second)).asScala.toList

    consumer.close()

    records
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    EmbeddedKafka.start()(testKafkaConfig)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    EmbeddedKafka.stop()
  }
}

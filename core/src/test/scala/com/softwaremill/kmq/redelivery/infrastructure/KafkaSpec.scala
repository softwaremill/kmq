package com.softwaremill.kmq.redelivery.infrastructure

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterEach, Suite}

trait KafkaSpec extends BeforeAndAfterEach { self: Suite =>

  val testKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2182)
  private implicit val stringDeserializer: StringDeserializer = new StringDeserializer()

  def sendToKafka(topic: String, message: String): Unit = {
    EmbeddedKafka.publishStringMessageToKafka(topic, message)(testKafkaConfig)
  }

  def consumeFromKafka(topic: String): String = {
    EmbeddedKafka.consumeFirstStringMessageFrom(topic)(testKafkaConfig)
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

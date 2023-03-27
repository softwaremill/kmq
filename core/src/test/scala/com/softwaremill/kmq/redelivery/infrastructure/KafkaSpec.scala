package com.softwaremill.kmq.redelivery.infrastructure

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait KafkaSpec extends BeforeAndAfterEach { self: Suite =>

  val testKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2182)

  def sendToKafka(topic: String, message: String): Unit = {
    EmbeddedKafka.publishStringMessageToKafka(topic, message)(testKafkaConfig)
  }

  def consumeFromKafka(topic: String): String = {
    EmbeddedKafka.consumeFirstStringMessageFrom(topic)(testKafkaConfig)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    EmbeddedKafka.start()(testKafkaConfig)
    ()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    EmbeddedKafka.stop()
  }
}

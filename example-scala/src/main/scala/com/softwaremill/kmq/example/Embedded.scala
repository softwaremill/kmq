package com.softwaremill.kmq.example

import com.softwaremill.kmq.{KafkaClients, KmqClient, KmqConfig, RedeliveryTracker}
import com.typesafe.scalalogging.StrictLogging
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteBufferDeserializer, ByteBufferSerializer}

import java.nio.ByteBuffer
import java.time.Duration
import java.util.Random
import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.jdk.CollectionConverters.IterableHasAsScala

object Embedded extends StrictLogging {
  private val TOTAL_MSGS = 4
  private val FAIL_RATIO = 0.5
  private val PARTITIONS = 1

  private implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig.defaultConfig

  private val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort
  private val kmqConfig = new KmqConfig(bootstrapServers, "queue", "markers", "kmq_client", "kmq_redelivery",
    Duration.ofSeconds(3).toMillis, 1000)
  private val clients = new KafkaClients(kmqConfig)
  private val random: Random = new Random(0)

  private val processedMessages = new ConcurrentHashMap[Integer, Integer]

  final def main(args: Array[String]): Unit = {
    EmbeddedKafka.start()
    // The offsets topic has the same # of partitions as the queue topic.
    EmbeddedKafka.createCustomTopic(kmqConfig.getMarkerTopic, partitions = PARTITIONS)
    EmbeddedKafka.createCustomTopic(kmqConfig.getMsgTopic, partitions = PARTITIONS)
    logger.info("Kafka started")

    val redelivery = RedeliveryTracker.start(clients, kmqConfig)
    startInBackground(() => processMessages(clients, kmqConfig))
    startInBackground(() => sendMessages(clients, kmqConfig))

    System.in.read // Wait for user input.

    redelivery.close()
    EmbeddedKafka.stop()
    logger.info("Kafka stopped")
  }

  private def sendMessages(clients: KafkaClients, kmqConfig: KmqConfig): Unit = {
    val msgProducer = clients.createProducer(classOf[ByteBufferSerializer], classOf[ByteBufferSerializer])

    logger.info("Sending ...")

    (0 until TOTAL_MSGS).foreach { i =>
      val data = ByteBuffer.allocate(4).putInt(i)
      msgProducer.send(new ProducerRecord(kmqConfig.getMsgTopic, data))
      sleep(100L)
    }

    msgProducer.close()

    logger.info("Sent")
  }

  private def processMessages(clients: KafkaClients, kmqConfig: KmqConfig): Unit = {
    val kmqClient = new KmqClient(kmqConfig, clients, classOf[ByteBufferDeserializer], classOf[ByteBufferDeserializer], Duration.ofMillis(100))
    val msgProcessingExecutor = Executors.newCachedThreadPool

    while (true) {
      for (record <- kmqClient.nextBatch.asScala) {
        msgProcessingExecutor.execute { () =>
          if (processMessage(record)) kmqClient.processed(record)
        }
      }
    }
  }

  private def processMessage(rawMsg: ConsumerRecord[ByteBuffer, ByteBuffer]): Boolean = {
    val msg = rawMsg.value.getInt
    // FAIL_RATIO of the messages are dropped
    if (random.nextDouble() >= FAIL_RATIO) {
      logger.info("Processing message: " + msg)
      sleep(random.nextInt(1500)) // Sleeping up to 1.5 seconds
      val previous = processedMessages.put(msg, msg)
      if (previous != null) {
        logger.warn(String.format("Message %d was already processed!", msg))
      } else {
        logger.info(String.format("Done processing message: %d. Total processed: %d/%d.", msg, processedMessages.size, TOTAL_MSGS))
      }
      true
    }
    else {
      logger.info("Dropping message: " + msg)
      false
    }
  }

  // ---

  private def startInBackground(r: Runnable): Unit = {
    val t = new Thread(r)
    t.setDaemon(true)
    t.start()
  }

  private def sleep(millis: Long): Unit = {
    try {
      Thread.sleep(millis)
    } catch {
      case e: InterruptedException => throw new RuntimeException(e)
    }
  }
}
package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{EndMarker, KafkaClients, KmqConfig, MarkerKey}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.nio.ByteBuffer
import java.time.Duration
import java.util.Collections
import java.util.concurrent.{Future, TimeUnit}
import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait Redeliverer {
  def redeliver(toRedeliver: List[MarkerKey]): Unit

  def close(): Unit
}

class DefaultRedeliverer(
    partition: Partition,
    producer: KafkaProducer[Array[Byte], Array[Byte]],
    config: KmqConfig,
    clients: KafkaClients
) extends Redeliverer
    with StrictLogging {

  private val SendTimeoutSeconds = 60L

  private val tp = new TopicPartition(config.getMsgTopic, partition)

  private val reader = {
    val c = clients.createConsumer(null, classOf[ByteArrayDeserializer], classOf[ByteArrayDeserializer])
    c.assign(Collections.singleton(tp))
    new SingleOffsetReader(tp, c)
  }

  def redeliver(toRedeliver: List[MarkerKey]): Unit = {
    toRedeliver
      .map(m => RedeliveredMarker(m, redeliver(m)))
      .foreach(rm => {
        rm.sendResult.get(SendTimeoutSeconds, TimeUnit.SECONDS)

        // ignoring the result, worst case if this fails the message will be re-processed after restart
        writeEndMarker(rm.marker)
      })
  }

  private def redeliver(marker: MarkerKey): Future[RecordMetadata] = {
    if (marker.getPartition != partition) {
      throw new IllegalStateException(
        s"Got marker key for partition ${marker.getPartition}, while the assigned partition is $partition!"
      )
    }

    reader.read(marker.getMessageOffset) match {
      case None =>
        throw new IllegalStateException(
          s"Cannot redeliver $marker from topic ${config.getMsgTopic} due to data fetch timeout"
        )

      case Some(toSend) =>
        val redeliveryCount = toSend.headers.asScala
          .find(_.key() == config.getRedeliveryCountHeader)
          .map(_.value())
          .map(decodeInt)
          .getOrElse(0)
        if (redeliveryCount < config.getMaxRedeliveryCount) {
          logger.info(
            s"Redelivering message from ${config.getMsgTopic}, partition ${marker.getPartition}, offset ${marker.getMessageOffset}, redelivery count $redeliveryCount"
          )
          val redeliveryHeader =
            Seq[Header](new RecordHeader(config.getRedeliveryCountHeader, encodeInt(redeliveryCount + 1))).asJava
          producer.send(new ProducerRecord(toSend.topic, toSend.partition, toSend.key, toSend.value, redeliveryHeader))
        } else {
          logger.warn(
            s"Redelivering message from ${config.getMsgTopic}, partition ${marker.getPartition}, offset ${marker.getMessageOffset}, redelivery count $redeliveryCount - max redelivery count of ${config.getMaxRedeliveryCount} exceeded; sending message to a dead-letter topic ${config.getDeadLetterTopic}"
          )
          producer.send(new ProducerRecord(config.getDeadLetterTopic, toSend.key, toSend.value))
        }
    }
  }

  private def writeEndMarker(marker: MarkerKey): Future[RecordMetadata] = {
    producer.send(
      new ProducerRecord(config.getMarkerTopic, partition, marker.serialize, EndMarker.INSTANCE.serialize())
    )
  }

  private case class RedeliveredMarker(marker: MarkerKey, sendResult: Future[RecordMetadata])

  def close(): Unit = reader.close()

  private def decodeInt(bytes: Array[Byte]): Int = {
    if (bytes.length == 1)
      bytes(0)
    else
      ByteBuffer.wrap(bytes).getInt
  }

  private def encodeInt(value: Int): Array[Byte] = {
    val byteValue = value.toByte
    if (byteValue == value)
      Array(byteValue)
    else
      ByteBuffer.allocate(4).putInt(value).array()
  }
}

class RetryingRedeliverer(delegate: Redeliverer) extends Redeliverer with StrictLogging {
  private val MaxBatch = 128
  private val MaxRetries = 16

  override def redeliver(toRedeliver: List[MarkerKey]): Unit = {
    tryRedeliver(toRedeliver.sortBy(_.getMessageOffset).grouped(MaxBatch).toList.map(RedeliveryBatch(_, 1)))
  }

  @tailrec
  private def tryRedeliver(batches: List[RedeliveryBatch]): Unit = {
    val batchesToRetry = batches.flatMap { batch =>
      try {
        delegate.redeliver(batch.markers)
        Nil // redelivered, nothing to retry
      } catch {
        case e: Exception if batch.retry < MaxRetries =>
          logger.warn(s"Exception when trying to redeliver ${batch.markers}. Will try again.", e)
          batch.markers.map(m => RedeliveryBatch(List(m), batch.retry + 1)) // retrying one-by-one
        case e: Exception =>
          logger.error(
            s"Exception when trying to redeliver ${batch.markers}. Tried $MaxRetries, Will not try again.",
            e
          )
          Nil
      }
    }

    if (batchesToRetry.nonEmpty) {
      tryRedeliver(batchesToRetry)
    }
  }

  override def close(): Unit = delegate.close()

  private case class RedeliveryBatch(markers: List[MarkerKey], retry: Int)
}

private class SingleOffsetReader(tp: TopicPartition, consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {
  private val PollTimeout = Duration.ofSeconds(100)

  private var cachedRecords: List[ConsumerRecord[Array[Byte], Array[Byte]]] = Nil

  def read(offset: Offset): Option[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    findInCache(offset)
      .orElse(seekAndRead(offset))
  }

  private def findInCache(offset: Offset) = {
    cachedRecords.find(_.offset() == offset)
  }

  private def seekAndRead(offset: Offset) = {
    consumer.seek(tp, offset)
    val pollResults = consumer.poll(PollTimeout)
    updateCache(pollResults)
    cachedRecords.headOption
  }

  private def updateCache(records: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = {
    cachedRecords = records.records(tp).asScala.toList
  }

  def close(): Unit = {
    consumer.close()
  }
}

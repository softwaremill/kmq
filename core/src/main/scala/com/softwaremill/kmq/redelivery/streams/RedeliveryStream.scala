package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.{DefaultRedeliverer, RetryingRedeliverer, Timestamp}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

class RedeliveryStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                       markersTopic: String, maxPartitions: Int,
                       kafkaClients: KafkaClients, kmqConfig: KmqConfig)
                      (implicit system: ActorSystem) extends StrictLogging {

  private val producer = kafkaClients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])

  //TODO: need to merge this stream with CommitMarkerOffsetsStream
  def run(): DrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
            .map(MarkerRedeliveryCommand)
            .merge(Source.tick(initialDelay = 1.second, interval = 1.second, tick = TickRedeliveryCommand))
            .statefulMapConcat { () => // keep track of open markers
              val markersByTimestamp = new CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestTimestampAscending)
              cmd => {
                logCommand(cmd)
                cmd match {
                  case TickRedeliveryCommand => // nothing to do
                  case MarkerRedeliveryCommand(msg) =>
                    msg.record.value match {
                      case _: StartMarker => markersByTimestamp.put(msg.record.key, msg)
                      case _: EndMarker => markersByTimestamp.remove(msg.record.key)
                      case _ => throw new IllegalArgumentException()
                    }
                }

                // pass on all expired markers
                val now = System.currentTimeMillis()
                logHeadOption(markersByTimestamp, now)

                val toRedeliver = ArrayBuffer[CommittableMessage[MarkerKey, MarkerValue]]()
                while (markersByTimestamp.headOption.exists(now >= _.record.value.asInstanceOf[StartMarker].getRedeliverAfter)) {
                  toRedeliver += markersByTimestamp.dequeue()
                }
                logToRedeliver(toRedeliver)
                toRedeliver
              }
            }
            .statefulMapConcat { () => // redeliver
              val redeliverer = new RetryingRedeliverer(new DefaultRedeliverer(topicPartition.partition, producer, kmqConfig, kafkaClients))
              msg => {
                redeliverer.redeliver(List(msg.record.key)) // TODO: maybe bulk redeliver
                Some(msg)
              }
            }
            .runWith(Sink.ignore)
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  private def bySmallestTimestampAscending(implicit ord: Ordering[Timestamp]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.value.asInstanceOf[StartMarker].getRedeliverAfter, x.record.value.asInstanceOf[StartMarker].getRedeliverAfter)

  private def logCommand(cmd: RedeliveryCommand): Unit = {
    logger.whenTraceEnabled {
      cmd match {
        case TickRedeliveryCommand => logger.trace(s"command: Tick")
        case MarkerRedeliveryCommand(msg) => logger.trace(s"command: ${markerToLogger(msg)})")
      }
    }
  }

  private def logHeadOption(markersByTimestamp: CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]], now: Timestamp): Unit = {
    logger.whenTraceEnabled {
      markersByTimestamp.headOption match {
        case Some(msg) => logger.trace(s"headOption: Some(${markerToLogger(msg)}), ${redeliveryTimeToLogger(msg, now)}")
        case None => logger.trace("headOption: None")
      }
    }
  }

  private def logToRedeliver(toRedeliver: Iterable[CommittableMessage[MarkerKey, MarkerValue]]): Unit = {
    logger.whenTraceEnabled {
      logger.trace(s"toRedeliver: ${toRedeliver.map(markerToLogger)}")
    }
  }

  private def markerToLogger(msg: CommittableMessage[MarkerKey, MarkerValue]): String =
    s"${msg.record.value.getClass.getSimpleName}(${msg.record.key.getPartition}, ${msg.record.key.getMessageOffset})"

  private def redeliveryTimeToLogger(msg: CommittableMessage[MarkerKey, MarkerValue], now: Timestamp): String =
    s"expected redelivery in = ${msg.record.value.asInstanceOf[StartMarker].getRedeliverAfter - now}ms"

  sealed trait RedeliveryCommand
  case object TickRedeliveryCommand extends RedeliveryCommand
  case class MarkerRedeliveryCommand(marker: CommittableMessage[MarkerKey, MarkerValue]) extends RedeliveryCommand
}
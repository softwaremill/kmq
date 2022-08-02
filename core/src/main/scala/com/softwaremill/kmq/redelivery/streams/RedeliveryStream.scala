package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.streams.RedeliveryStream._
import com.softwaremill.kmq.redelivery.{DefaultRedeliverer, Partition, RetryingRedeliverer, Timestamp}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RedeliveryStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                       markersTopic: String, maxPartitions: Int,
                       kafkaClients: KafkaClients, kmqConfig: KmqConfig)
                      (implicit system: ActorSystem) extends StrictLogging {

  private val producer = kafkaClients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])

  def redeliverySink(partition: Partition): Sink[CommittableMessage[MarkerKey, MarkerValue], Future[Done]] = {
    Flow[CommittableMessage[MarkerKey, MarkerValue]]
      .map(MarkerRedeliveryCommand)
      .merge(Source.tick(initialDelay = 1.second, interval = 1.second, tick = TickRedeliveryCommand))
      .statefulMapConcat { () => // keep track of open markers
        val markersByTimestamp = new PriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestTimestampAscending)
        val latestMarkerSeenTimestamp = new Ref[Timestamp]
        val latestMarkerSeenAt = new Ref[Timestamp]
        cmd => {
          logger.traceCommand(cmd)
          cmd match {
            case TickRedeliveryCommand => // nothing to do
            case MarkerRedeliveryCommand(msg) =>
              // update markersByTimestamp
              msg.record.value match {
                case _: StartMarker => markersByTimestamp.put(msg.record.key, msg)
                case _: EndMarker => markersByTimestamp.remove(msg.record.key)
                case _ => throw new IllegalArgumentException()
              }

              //update latestMarkerSeen timestamps
              latestMarkerSeenAt.update(System.currentTimeMillis())
              if (!latestMarkerSeenTimestamp.getOption.exists(_ >= msg.record.timestamp)) {
                latestMarkerSeenTimestamp.update(msg.record.timestamp)
              }
          }

          // pass on all expired markers TODO: cleanup
          val currentTime = System.currentTimeMillis()
          val now = latestMarkerSeenAt.getOption.flatMap { lm =>
            if (currentTime - lm < kmqConfig.getUseNowForRedeliverDespiteNoMarkerSeenForMs) {
              /* If we've seen a marker recently, then using the latest seen marker (which is the maximum marker offset seen
              at all) for computing redelivery. This guarantees that we won't redeliver a message for which an end marker
              was sent, but is waiting in the topic for being observed, even though comparing the wall clock time and start
              marker timestamp exceeds the message timeout. */
              latestMarkerSeenTimestamp.getOption
            } else {
              /* If we haven't seen a marker recently, assuming that it's because all available have been observed. Hence
              there are no delays in processing of the markers, so we can use the current time for computing which messages
              should be redelivered. */
              Some(currentTime)
            }
          }.getOrElse(currentTime)
          logger.traceHeadOption(markersByTimestamp, now)

          val toRedeliver = ArrayBuffer[CommittableMessage[MarkerKey, MarkerValue]]()
          while (markersByTimestamp.headOption.exists(now >= _.record.value.asInstanceOf[StartMarker].getRedeliverAfter)) {
            toRedeliver += markersByTimestamp.dequeue()
          }
          logger.traceToRedeliver(toRedeliver)
          toRedeliver
        }
      }
      .statefulMapConcat { () => // redeliver
        val redeliverer = new RetryingRedeliverer(new DefaultRedeliverer(partition, producer, kmqConfig, kafkaClients))
        msg => {
          redeliverer.redeliver(List(msg.record.key)) // TODO: maybe bulk redeliver
          Some(Done)
        }
      }
      .toMat(Sink.ignore)(Keep.right)
  }

  def run(): DrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
            .toMat(redeliverySink(topicPartition.partition))(Keep.right)
            .run()
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  private def bySmallestTimestampAscending(implicit ord: Ordering[Timestamp]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.value.asInstanceOf[StartMarker].getRedeliverAfter, x.record.value.asInstanceOf[StartMarker].getRedeliverAfter)
}

object RedeliveryStream {

  sealed trait RedeliveryCommand
  case object TickRedeliveryCommand extends RedeliveryCommand
  case class MarkerRedeliveryCommand(marker: CommittableMessage[MarkerKey, MarkerValue]) extends RedeliveryCommand

  implicit class LoggerOperations(logger: Logger) {

    def traceCommand(cmd: RedeliveryCommand): Unit = {
      logger.whenTraceEnabled {
        cmd match {
          case TickRedeliveryCommand => logger.trace(s"command: Tick")
          case MarkerRedeliveryCommand(msg) => logger.trace(s"command: ${markerToLogger(msg)})")
        }
      }
    }

    def traceHeadOption(markersByTimestamp: PriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]], now: Timestamp): Unit = {
      logger.whenTraceEnabled {
        markersByTimestamp.headOption match {
          case Some(msg) => logger.trace(s"headOption: Some(${markerToLogger(msg)}), ${redeliveryTimeToLogger(msg, now)}")
          case None => logger.trace("headOption: None")
        }
      }
    }

    def traceToRedeliver(toRedeliver: Iterable[CommittableMessage[MarkerKey, MarkerValue]]): Unit = {
      logger.whenTraceEnabled {
        logger.trace(s"toRedeliver: ${toRedeliver.map(markerToLogger)}")
      }
    }

    private def markerToLogger(msg: CommittableMessage[MarkerKey, MarkerValue]): String =
      s"${msg.record.value.getClass.getSimpleName}(${msg.record.key.getPartition}, ${msg.record.key.getMessageOffset})"

    private def redeliveryTimeToLogger(msg: CommittableMessage[MarkerKey, MarkerValue], now: Timestamp): String =
      s"expected redelivery in = ${msg.record.value.asInstanceOf[StartMarker].getRedeliverAfter - now}ms"
  }
}
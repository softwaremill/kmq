package com.softwaremill.kmq.redelivery.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.{DefaultRedeliverer, Partition, RetryingRedeliverer, Timestamp}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.apache.kafka.common.serialization.ByteArraySerializer

import java.time.{Clock, Instant}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

object RedeliverySink extends StrictLogging {

  def apply(partition: Partition)
           (implicit system: ActorSystem, kafkaClients: KafkaClients, kmqConfig: KmqConfig, clock: Clock
           ): Sink[CommittableMessage[MarkerKey, MarkerValue], NotUsed] = {
    val producer = kafkaClients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])
    val redeliverer = new RetryingRedeliverer(new DefaultRedeliverer(partition, producer, kmqConfig, kafkaClients))

    Flow[CommittableMessage[MarkerKey, MarkerValue]]
      .map(MarkerRedeliveryCommand)
      .merge(Source.tick(initialDelay = 1.second, interval = 1.second, tick = TickRedeliveryCommand))
      .statefulMapConcat { () => // keep track of open markers; select markers to redeliver
        val markersByTimestamp = new PriorityQueueMap[MarkerKey, MsgWithTimestamp](valueOrdering = bySmallestTimestampAscending)
        var latestMarkerSeenTimestamp: Option[Timestamp] = None
        var latestMarkerSeenAt: Option[Timestamp] = None
        cmd => {
          logger.traceCommand(cmd)
          cmd match {
            case TickRedeliveryCommand => // nothing to do
            case MarkerRedeliveryCommand(msg) =>
              // update markersByTimestamp
              msg.record.value match {
                case start: StartMarker => markersByTimestamp.put(msg.record.key, MsgWithTimestamp(msg, msg.record.timestamp + start.getRedeliverAfter))
                case _: EndMarker => markersByTimestamp.remove(msg.record.key)
                case _ => throw new IllegalArgumentException()
              }

              //update latestMarkerSeen timestamps
              latestMarkerSeenAt = Some(Instant.now(clock).toEpochMilli)
              if (!latestMarkerSeenTimestamp.exists(_ >= msg.record.timestamp)) {
                latestMarkerSeenTimestamp = Some(msg.record.timestamp)
              }
          }

          // pass on all expired markers TODO: cleanup
          val currentTime = Instant.now(clock).toEpochMilli
          val now = latestMarkerSeenAt.flatMap { lm =>
            if (currentTime - lm < kmqConfig.getUseNowForRedeliverDespiteNoMarkerSeenForMs) {
              /* If we've seen a marker recently, then using the latest seen marker (which is the maximum marker offset seen
              at all) for computing redelivery. This guarantees that we won't redeliver a message for which an end marker
              was sent, but is waiting in the topic for being observed, even though comparing the wall clock time and start
              marker timestamp exceeds the message timeout. */
              latestMarkerSeenTimestamp
            } else {
              /* If we haven't seen a marker recently, assuming that it's because all available have been observed. Hence
              there are no delays in processing of the markers, so we can use the current time for computing which messages
              should be redelivered. */
              Some(currentTime)
            }
          }.getOrElse(currentTime)
          logger.traceHeadOption(markersByTimestamp, now)

          val toRedeliver = ArrayBuffer[CommittableMessage[MarkerKey, MarkerValue]]()
          while (markersByTimestamp.headOption.exists(now >= _.redeliveryTime)) {
            toRedeliver += markersByTimestamp.dequeue().msg
          }
          logger.traceToRedeliver(toRedeliver)
          toRedeliver
        }
      }
      .toMat(Sink.foreach { msg => // redeliver
          redeliverer.redeliver(List(msg.record.key)) // TODO: maybe bulk redeliver

      })(Keep.left)
  }

  private def bySmallestTimestampAscending(implicit ord: Ordering[Timestamp]): Ordering[MsgWithTimestamp] =
    (x, y) => ord.compare(x.redeliveryTime, y.redeliveryTime)

  case class MsgWithTimestamp(msg: CommittableMessage[MarkerKey, MarkerValue], redeliveryTime: Timestamp)

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

    def traceHeadOption(markersByTimestamp: PriorityQueueMap[MarkerKey, MsgWithTimestamp], now: Timestamp): Unit = {
      logger.whenTraceEnabled {
        markersByTimestamp.headOption match {
          case Some(head) => logger.trace(s"headOption: Some(${markerToLogger(head.msg)}), ${redeliveryTimeToLogger(head.redeliveryTime, now)}")
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

    private def redeliveryTimeToLogger(redeliveryTime: Timestamp, now: Timestamp): String =
      s"expected redelivery in = ${redeliveryTime - now}ms"
  }
}
package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import com.softwaremill.kmq._
import com.softwaremill.kmq.redelivery.streams.RedeliveryStream._
import com.softwaremill.kmq.redelivery.{DefaultRedeliverer, Offset, RetryingRedeliverer, Timestamp}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class RedeliveryStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                       markersTopic: String, maxPartitions: Int,
                       kafkaClients: KafkaClients, kmqConfig: KmqConfig)
                      (implicit system: ActorSystem, ec: ExecutionContext) extends StrictLogging {

  private val producer = kafkaClients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])

  // TODO: should combine functionality of RedeliverSimpleStream and CommitMarkerOffsetsStream
  def run(): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>

          val redeliverySink = Flow[CommittableMessage[MarkerKey, MarkerValue]]
            .map(MarkerRedeliveryCommand)
            .merge(Source.tick(initialDelay = 1.second, interval = 1.second, tick = TickRedeliveryCommand))
            .statefulMapConcat { () => // keep track of open markers
              val markersByTimestamp = new CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestTimestampAscending)
              cmd => {
                logger.traceCommand(cmd)
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
              val redeliverer = new RetryingRedeliverer(new DefaultRedeliverer(topicPartition.partition, producer, kmqConfig, kafkaClients))
              msg => {
                redeliverer.redeliver(List(msg.record.key)) // TODO: maybe bulk redeliver
                Some(msg)
              }
            }
            .toMat(Sink.ignore)(Keep.right)

          val commitMarkerOffsetsSink = Flow[CommittableMessage[MarkerKey, MarkerValue]]
            .statefulMapConcat { () => // keep track of open markers
              val markersByOffset = new CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestOffsetAscending)
              msg => {
                msg.record.value match {
                  case _: StartMarker => markersByOffset.put(msg.record.key, msg)
                  case _: EndMarker => markersByOffset.remove(msg.record.key)
                  case _ => throw new IllegalArgumentException()
                }
                markersByOffset.headOption
              }
            }
            .statefulMapConcat { () => // deduplicate - pass only markers with increasing offsets
              val maxOffset = new CustomHolder[Offset]()
              msg =>
                if (!maxOffset.getOption.exists(_ >= msg.record.offset)) {
                  maxOffset.update(msg.record.offset)
                  Some(msg)
                }
                else None
            }
            .statefulMapConcat { () => // for each new marker return previous one
              val previousMsg = new CustomHolder[CommittableMessage[MarkerKey, MarkerValue]]()
              msg =>
                val prev = previousMsg.getOption
                previousMsg.update(msg)
                prev
            }
            .map(_.committableOffset)
            .via(Committer.flow(committerSettings))
            .toMat(Sink.ignore)(Keep.right)

          RunnableGraph
            .fromGraph(GraphDSL.createGraph(redeliverySink, commitMarkerOffsetsSink)(combineFutures) {
              implicit builder => (sink1, sink2) =>
                import GraphDSL.Implicits._
                val broadcast = builder.add(Broadcast[CommittableMessage[MarkerKey, MarkerValue]](2))
                source ~> broadcast
                broadcast.out(0) ~> Flow[CommittableMessage[MarkerKey, MarkerValue]].async ~> sink1
                broadcast.out(1) ~> Flow[CommittableMessage[MarkerKey, MarkerValue]].async ~> sink2
                ClosedShape
            })
            .run()
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  private def bySmallestOffsetAscending(implicit ord: Ordering[Offset]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.offset, x.record.offset)

  private def bySmallestTimestampAscending(implicit ord: Ordering[Timestamp]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.value.asInstanceOf[StartMarker].getRedeliverAfter, x.record.value.asInstanceOf[StartMarker].getRedeliverAfter)

  private def combineFutures(l: Future[Done], r: Future[Done]): Future[Done] = {
    Future.sequence(Seq(l, r)).map(_ => Done)
  }
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

    def traceHeadOption(markersByTimestamp: CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]], now: Timestamp): Unit = {
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
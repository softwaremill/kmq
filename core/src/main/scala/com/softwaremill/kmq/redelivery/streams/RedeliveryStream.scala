package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.kmq.redelivery.{DefaultRedeliverer, RetryingRedeliverer, Timestamp}
import com.softwaremill.kmq._
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

class RedeliveryStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                       markersTopic: String, maxPartitions: Int,
                       kafkaClients: KafkaClients, kmqConfig: KmqConfig)
                      (implicit system: ActorSystem) {

  val producer = kafkaClients.createProducer(classOf[ByteArraySerializer], classOf[ByteArraySerializer])

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
                val toRedeliver = ArrayBuffer[CommittableMessage[MarkerKey, MarkerValue]]()
                while (markersByTimestamp.headOption.exists(now >= _.record.value.asInstanceOf[StartMarker].getRedeliverAfter)) {
                  toRedeliver += markersByTimestamp.dequeue()
                }
                toRedeliver
              }
            }
            .statefulMapConcat { () => // redeliver
              val redeliverer = new RetryingRedeliverer(new DefaultRedeliverer(topicPartition.partition, producer, kmqConfig, kafkaClients))
              msg => redeliverer.redeliver(List(msg.record.key)) // TODO: maybe bulk redeliver
              Some(Done)
            }
            .runWith(Sink.ignore)
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  def bySmallestTimestampAscending(implicit ord: Ordering[Timestamp]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.value.asInstanceOf[StartMarker].getRedeliverAfter, x.record.value.asInstanceOf[StartMarker].getRedeliverAfter)

  sealed trait RedeliveryCommand
  case object TickRedeliveryCommand extends RedeliveryCommand
  case class MarkerRedeliveryCommand(marker: CommittableMessage[MarkerKey, MarkerValue]) extends RedeliveryCommand
}
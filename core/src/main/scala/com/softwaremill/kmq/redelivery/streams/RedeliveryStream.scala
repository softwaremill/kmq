package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.kmq.redelivery.Timestamp
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

import scala.concurrent.duration.DurationInt

class RedeliveryStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                       markersTopic: String, maxPartitions: Int)
                      (implicit system: ActorSystem) {

  //TODO: divertToMat
  def run(): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
            .map(MarkerRedeliveryCommand)
            .merge(Source.tick(initialDelay = 1.second, interval = 1.second, tick = TickRedeliveryCommand))
            .statefulMapConcat { () =>
              val markersByTimestamp = new CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestTimestampAscending)
              cmd => {
                cmd match {
                  case TickRedeliveryCommand => // pass on
                  case MarkerRedeliveryCommand(msg) =>
                    msg.record.value match {
                      case _: StartMarker => markersByTimestamp.put(msg.record.key, msg)
                      case _: EndMarker => markersByTimestamp.remove(msg.record.key)
                      case _ => throw new IllegalArgumentException()
                    }
                }

                //                val head = markersByTimestamp.headOption.get
                //                head.record.value.asInstanceOf[StartMarker].getRedeliverAfter

                markersByTimestamp.headOption
              }
              //
              //        // ... pobieram (dequeue) wszystkie markery z `markersByTimestamp`, dla których redeliverAfter < now; zwracam kolekcję markerów
              //      }
              //      .via {
              //        // ... Flow z użyciem istniejącego `Redeliverer`a; Producer wysyła ponownie wiadomości i nowe `StartMarker`y z nowym redeliverAfter
              //      }
              //      .to(Sink.ignore) // nie commituję offsetów dla markerów

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
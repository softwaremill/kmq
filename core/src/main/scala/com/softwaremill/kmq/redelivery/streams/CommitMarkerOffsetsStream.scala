package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.softwaremill.kmq.redelivery.Offset
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

import scala.concurrent.Future

class CommitMarkerOffsetsStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                                markersTopic: String, maxPartitions: Int)
                               (implicit system: ActorSystem) {
  
  def commitMarkerOffsetsSink(): Sink[CommittableMessage[MarkerKey, MarkerValue], Future[Done]] = {
    val committerSettings = CommitterSettings(system)

    Flow[CommittableMessage[MarkerKey, MarkerValue]]
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
  }
  
  def run(): DrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
            .toMat(commitMarkerOffsetsSink())(Keep.right)
            .run()
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  def bySmallestOffsetAscending(implicit ord: Ordering[Offset]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.offset, x.record.offset)
}
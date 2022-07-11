package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import com.softwaremill.kmq.redelivery.Offset
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

object CommitMarkerOffsetsStream {

  def run(consumerSettings: ConsumerSettings[MarkerKey, MarkerValue], markersTopic: String, maxPartitions: Int)
         (implicit system: ActorSystem): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committableSource(consumerSettings, Subscriptions.topics(markersTopic))
      .groupBy(maxSubstreams = maxPartitions, f = msg => msg.record.partition) // sorted not on msg.record.key.messagePartition
      .statefulMapConcat { () =>
        val markersByOffset = new CustomPriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestOffsetAscending)
        msg => {
          msg.record.value match {
            case StartMarker =>
              markersByOffset.put(msg.record.key, msg)
            case EndMarker =>
              markersByOffset.remove(msg.record.key)
          }
          markersByOffset.headOption
        }
      }
      .statefulMapConcat { () => // not sure if this step is required
        val maxOffset = new CustomHolder[Offset]()
        msg =>
          if (maxOffset.get.fold(true)(_ < msg.record.offset)) {
            maxOffset.update(msg.record.offset)
            Some(msg)
          }
          else None
      }
      .mergeSubstreams
      .map(_.committableOffset)
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()
  }

  def bySmallestOffsetAscending(implicit ord: Ordering[Offset]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.offset, x.record.offset)
}

package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import com.softwaremill.kmq.redelivery.Offset
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

class CommitMarkerOffsetsStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                                markersTopic: String, maxPartitions: Int)
                               (implicit system: ActorSystem) {

  def run(): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
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
                if (maxOffset.get.fold(true)(_ < msg.record.offset)) {
                  maxOffset.update(msg.record.offset)
                  Some(msg)
                }
                else None
            }
            .statefulMapConcat { () => // for each new marker return previous one
              val previousMsg = new CustomHolder[CommittableMessage[MarkerKey, MarkerValue]]()
              msg =>
                val prev = previousMsg.get
                previousMsg.update(msg)
                prev
            }
            .map(_.committableOffset)
            .runWith(Committer.sink(committerSettings))
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  def bySmallestOffsetAscending(implicit ord: Ordering[Offset]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.offset, x.record.offset)
}
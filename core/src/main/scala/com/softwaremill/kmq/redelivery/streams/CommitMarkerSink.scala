package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Committer
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.softwaremill.kmq.redelivery.Offset
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

import scala.concurrent.Future

object CommitMarkerSink {
  
  def apply()(implicit system: ActorSystem): Sink[CommittableMessage[MarkerKey, MarkerValue], Future[Done]] = {
    val committerSettings = CommitterSettings(system)

    Flow[CommittableMessage[MarkerKey, MarkerValue]]
      .statefulMapConcat { () => // keep track of open markers
        val markersByOffset = new PriorityQueueMap[MarkerKey, CommittableMessage[MarkerKey, MarkerValue]](valueOrdering = bySmallestOffsetAscending)
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
        var maxOffset: Option[Offset] = None
        msg =>
          if (!maxOffset.exists(_ >= msg.record.offset)) {
            maxOffset = Some(msg.record.offset)
            Some(msg)
          }
          else None
      }
      .statefulMapConcat { () => // for each new marker return previous one
        var previousMsg: Option[CommittableMessage[MarkerKey, MarkerValue]] = None
        msg =>
          val prev = previousMsg
          previousMsg = Some(msg)
          prev
      }
      .map(_.committableOffset)
      .toMat(Committer.sink(committerSettings))(Keep.right)
  }

  def bySmallestOffsetAscending(implicit ord: Ordering[Offset]): Ordering[CommittableMessage[MarkerKey, MarkerValue]] =
    (x, y) => ord.compare(y.record.offset, x.record.offset)
}
package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

import scala.collection.mutable

class MarkersQueue(disableRedeliveryBefore: Offset) {
  private val markersInProgress = mutable.Set[MarkerKey]()
  private val markersByTimestamp = new mutable.PriorityQueue[(MarkerKey, Timestamp)]()(bySecondTupleOrdering)
  private val markersByOffset = new mutable.PriorityQueue[(MarkerKey, Offset)]()(bySecondTupleOrdering)
  private var redeliveryEnabled = false

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue, t: Timestamp) {
    if (markerOffset >= disableRedeliveryBefore) {
      redeliveryEnabled = true
    }

    v match {
      case s: StartMarker =>
        markersByOffset.enqueue((k, markerOffset))
        markersByTimestamp.enqueue((k, t+s.getRedeliverAfter))
        markersInProgress += k

      case _: EndMarker =>
        markersInProgress -= k
    }
  }

  def markersToRedeliver(now: Timestamp): List[MarkerKey] = {
    removeEndedMarkers(markersByTimestamp)

    var toRedeliver = List.empty[MarkerKey]

    if (redeliveryEnabled) {
      while (shouldRedeliverMarkersQueueHead(now)) {
        val queueHead = markersByTimestamp.dequeue()
        // the first marker, if any, is not ended for sure (b/c of the cleanup that's done at the beginning),
        // but subsequent markers don't have to be.
        if (markersInProgress.contains(queueHead._1)) {
          toRedeliver ::= queueHead._1
        }

        // not removing from markersInProgress - until we are sure the message is redelivered (the redeliverer
        // sends an end marker when this is done) - the marker needs to stay for minimum-offset calculations to be
        // correct
      }
    }

    toRedeliver
  }

  def smallestMarkerOffset(): Option[Offset] = {
    removeEndedMarkers(markersByOffset)
    markersByOffset.headOption.map(_._2)
  }

  private def removeEndedMarkers[T](queue: mutable.PriorityQueue[(MarkerKey, T)]): Unit = {
    while (isHeadEnded(queue)) {
      queue.dequeue()
    }
  }

  private def isHeadEnded[T](queue: mutable.PriorityQueue[(MarkerKey, T)]): Boolean = {
    queue.headOption.exists(e => !markersInProgress.contains(e._1))
  }

  private def shouldRedeliverMarkersQueueHead(now: Timestamp): Boolean = {
    markersByTimestamp.headOption match {
      case None => false
      case Some(m) => now >= m._2
    }
  }

  private def bySecondTupleOrdering[T, U: Ordering]: Ordering[(T, U)] = new Ordering[(T, U)] {
    override def compare(x: (T, U), y: (T, U)): Int = {
      implicitly[Ordering[U]].compare(x._2, y._2)
    }
  }
}
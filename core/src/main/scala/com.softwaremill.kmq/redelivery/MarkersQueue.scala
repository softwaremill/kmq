package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}
import java.time.Clock

import scala.collection.mutable

class MarkersQueue(clock: Clock, disableRedeliveryBefore: Offset) {
  private val markersInProgress = mutable.Map[MarkerKey, StartMarker]()
  private val markersByTimestamp = new mutable.PriorityQueue[Marker]()
  private val markersOffsets = new mutable.PriorityQueue[MarkerKeyWithOffset]()
  private var redeliveryEnabled = false

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue) {
    if (markerOffset >= disableRedeliveryBefore) {
      redeliveryEnabled = true
    }

    v match {
      case s: StartMarker =>
        markersOffsets.enqueue(MarkerKeyWithOffset(markerOffset, k))

        markersByTimestamp.enqueue(Marker(k, s))
        markersInProgress.put(k, s)

      case _: EndMarker =>
        markersInProgress.remove(k)
    }
  }

  def markersToRedeliver(): List[Marker] = {
    removeEndedMarkers(markersByTimestamp)(_.key)

    var toRedeliver = List.empty[Marker]

    if (redeliveryEnabled) {
      while (shouldRedeliverMarkersQueueHead()) {
        val queueHead = markersByTimestamp.dequeue()
        // the first marker, if any, is not ended for sure (b/c of the cleanup that's done at the beginning),
        // but subsequent markers don't have to be.
        if (markersInProgress.contains(queueHead.key)) {
          toRedeliver ::= queueHead
        }

        // not removing from markersInProgress - until we are sure the message is redelivered (the redeliverer
        // sends an end marker when this is done) - the marker needs to stay for minimum-offset calculations to be
        // correct
      }
    }

    toRedeliver
  }

  def smallestMarkerOffset(): Option[Offset] = {
    removeEndedMarkers(markersOffsets)(_.key)
    markersOffsets.headOption.map(_.markerOffset)
  }

  private def removeEndedMarkers[T](queue: mutable.PriorityQueue[T])(getKey: T => MarkerKey): Unit = {
    while (isHeadEnded(queue, getKey)) {
      queue.dequeue()
    }
  }

  private def isHeadEnded[T](queue: mutable.PriorityQueue[T], getKey: T => MarkerKey): Boolean = {
    queue.headOption.exists(e => !markersInProgress.contains(getKey.apply(e)))
  }

  private def shouldRedeliverMarkersQueueHead(): Boolean = {
    markersByTimestamp.headOption match {
      case None => false
      case Some(m) => clock.millis() >= m.value.getRedeliverTimestamp
    }
  }

  private case class MarkerKeyWithOffset(markerOffset: Offset, key: MarkerKey) extends Comparable[MarkerKeyWithOffset] {
    def compareTo(o: MarkerKeyWithOffset): Int = {
      val diff = markerOffset - o.markerOffset
      if (diff == 0L) 0 else if (diff < 0L) -1 else 1
    }
  }
}
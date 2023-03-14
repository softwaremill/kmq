package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

import scala.collection.mutable

class MarkersQueue(disableRedeliveryBefore: Offset) {
  private val markersInProgress = mutable.Set[MarkerKey]()
  private val markersByTimestamp =
    new mutable.PriorityQueue[AttributedMarkerKey[Timestamp]]()(bySmallestAttributeOrdering)
  private val markersByOffset = new mutable.PriorityQueue[AttributedMarkerKey[Offset]]()(bySmallestAttributeOrdering)
  private var redeliveryEnabled = false

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue, t: Timestamp): Unit = {
    if (markerOffset >= disableRedeliveryBefore) {
      redeliveryEnabled = true
    }

    v match {
      case s: StartMarker =>
        markersByOffset.enqueue(AttributedMarkerKey(k, markerOffset))
        markersByTimestamp.enqueue(AttributedMarkerKey(k, t + s.getRedeliverAfter))
        markersInProgress += k

      case _: EndMarker =>
        markersInProgress -= k

      case x => throw new IllegalArgumentException(s"Unknown marker type: ${x.getClass}")
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
        if (markersInProgress.contains(queueHead.key)) {
          toRedeliver ::= queueHead.key
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
    markersByOffset.headOption.map(_.attr)
  }

  private def removeEndedMarkers[T](queue: mutable.PriorityQueue[AttributedMarkerKey[T]]): Unit = {
    while (isHeadEnded(queue)) {
      queue.dequeue()
    }
  }

  private def isHeadEnded[T](queue: mutable.PriorityQueue[AttributedMarkerKey[T]]): Boolean = {
    queue.headOption.exists(e => !markersInProgress.contains(e.key))
  }

  private def shouldRedeliverMarkersQueueHead(now: Timestamp): Boolean = {
    markersByTimestamp.headOption match {
      case None    => false
      case Some(m) => now >= m.attr
    }
  }

  private case class AttributedMarkerKey[T](key: MarkerKey, attr: T)

  private def bySmallestAttributeOrdering[T: Ordering]: Ordering[AttributedMarkerKey[T]] =
    new Ordering[AttributedMarkerKey[T]] {
      override def compare(x: AttributedMarkerKey[T], y: AttributedMarkerKey[T]): Int = {
        -implicitly[Ordering[T]].compare(x.attr, y.attr)
      }
    }
}

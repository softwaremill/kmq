package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.redelivery.streams.PriorityQueueMap
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

class MarkersQueue(disableRedeliveryBefore: Offset) {
  private val markersByTimestamp = new PriorityQueueMap[MarkerKey, AttributedMarkerKey[Timestamp]](valueOrdering = bySmallestAttributeOrdering)
  private val markersByOffset = new PriorityQueueMap[MarkerKey, AttributedMarkerKey[Offset]](valueOrdering = bySmallestAttributeOrdering)
  private var redeliveryEnabled = false

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue, t: Timestamp): Unit = {
    if (markerOffset >= disableRedeliveryBefore) {
      redeliveryEnabled = true
    }

    v match {
      case s: StartMarker =>
        markersByOffset.put(k, AttributedMarkerKey(k, markerOffset))
        markersByTimestamp.put(k, AttributedMarkerKey(k, t + s.getRedeliverAfter))

      case _: EndMarker =>
        markersByOffset.remove(k)
        markersByTimestamp.remove(k)

      case x => throw new IllegalArgumentException(s"Unknown marker type: ${x.getClass}")
    }
  }

  def markersToRedeliver(now: Timestamp): List[MarkerKey] = {
    var toRedeliver = List.empty[MarkerKey]

    if (redeliveryEnabled) {
      while (shouldRedeliverMarkersQueueHead(now)) {
        toRedeliver ::= markersByTimestamp.dequeue().key

        // not removing from markersInProgress - until we are sure the message is redelivered (the redeliverer
        // sends an end marker when this is done) - the marker needs to stay for minimum-offset calculations to be
        // correct
      }
    }

    toRedeliver
  }

  def smallestMarkerOffset(): Option[Offset] = {
    markersByOffset.headOption.map(_.attr)
  }

  private def shouldRedeliverMarkersQueueHead(now: Timestamp): Boolean = {
    markersByTimestamp.headOption match {
      case None => false
      case Some(m) => now >= m.attr
    }
  }

  private case class AttributedMarkerKey[T](key: MarkerKey, attr: T)

  private def bySmallestAttributeOrdering[T](implicit ord: Ordering[T]): Ordering[AttributedMarkerKey[T]] =
    (x, y) => ord.compare(y.attr, x.attr)
}
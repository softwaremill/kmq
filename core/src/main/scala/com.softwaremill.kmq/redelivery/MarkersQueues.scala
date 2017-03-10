package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{MarkerKey, MarkerValue}

class MarkersQueues {
  private var markersQueues = Map[Partition, MarkersQueue]()

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue, t: Timestamp): Unit = {
    markersQueues.get(k.getPartition).foreach(_.handleMarker(markerOffset, k, v, t))
  }

  def smallestMarkerOffsetsPerPartition(): Map[Partition, Offset] = {
    markersQueues.flatMap { case (p, mq) =>
      mq.smallestMarkerOffset().map((p, _))
    }
  }

  def markersToRedeliver(p: Partition, now: Timestamp): List[MarkerKey] = {
    markersQueues.get(p).map(_.markersToRedeliver(now)).getOrElse(Nil)
  }

  def addPartition(p: Partition, currentLastMarkerOffset: Offset): Unit = {
    // Enabling redelivery only after the queue state if fully recovered, that is after it has observed all offsets
    // currently in the markers topic. That way we avoid redelivery of already processed messages.
    markersQueues += p -> new MarkersQueue(currentLastMarkerOffset)
  }

  def removePartition(p: Partition): Unit = {
    markersQueues -= p

  }
}
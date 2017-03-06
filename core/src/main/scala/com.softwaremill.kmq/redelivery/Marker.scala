package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{MarkerKey, StartMarker}

case class Marker(key: MarkerKey, value: StartMarker) extends Comparable[Marker] {
  def compareTo(o: Marker): Int = {
    val diff = value.getRedeliverTimestamp - o.value.getRedeliverTimestamp
    if (diff == 0L) 0 else if (diff < 0L) -1 else 1
  }
}

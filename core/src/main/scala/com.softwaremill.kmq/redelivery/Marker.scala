package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{MarkerKey, MarkerValue}

case class Marker(key: MarkerKey, value: MarkerValue) extends Comparable[Marker] {
  def compareTo(o: Marker): Int = {
    val diff = value.getProcessingTimestamp - o.value.getProcessingTimestamp
    if (diff == 0L) 0 else if (diff < 0L) -1 else 1
  }
}

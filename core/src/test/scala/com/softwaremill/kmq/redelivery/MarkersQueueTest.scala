package com.softwaremill.kmq.redelivery

import com.softwaremill.kmq.{EndMarker, MarkerKey, StartMarker}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class MarkersQueueTest extends AnyFlatSpec {
  it should "redeliver oldest markers when newer are present" in {
    // given
    val mq = new MarkersQueue(0)
    mq.handleMarker(100, new MarkerKey(1, 1000), new StartMarker(4000L), 10000L)
    mq.handleMarker(101, new MarkerKey(1, 1001), new StartMarker(4000L), 12000L)
    mq.handleMarker(102, new MarkerKey(1, 1002), new StartMarker(4000L), 13000L)
    mq.handleMarker(103, new MarkerKey(1, 1002), EndMarker.INSTANCE, 13000L)

    // when
    val toRedeliver = mq.markersToRedeliver(15000)

    // then
    toRedeliver should have size 1
    toRedeliver.head.getMessageOffset should be(1000)
  }

  it should "redeliver multiple markers" in {
    // given
    val mq = new MarkersQueue(0)
    mq.handleMarker(100, new MarkerKey(1, 1000), new StartMarker(4000L), 10000L)
    mq.handleMarker(101, new MarkerKey(1, 1001), new StartMarker(4000L), 12000L)

    // when
    val toRedeliver = mq.markersToRedeliver(17000)

    // then
    toRedeliver should have size 2
    toRedeliver.map(_.getMessageOffset).toSet should be(Set(1001L, 1000L))
  }
}

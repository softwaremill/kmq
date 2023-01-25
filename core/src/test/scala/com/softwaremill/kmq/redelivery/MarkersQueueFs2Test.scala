package com.softwaremill.kmq.redelivery

import cats.Order
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.PQueue
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.kernel.Order.fromOrdering
import com.softwaremill.kmq.redelivery.model.AttributedMarkerKey
import com.softwaremill.kmq.redelivery.utils.bySmallestAttributeOrdering
import com.softwaremill.kmq.{MarkerKey, StartMarker}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable

class MarkersQueueFs2Test extends AsyncFlatSpec with AsyncIOSpec {
  it should "redeliver oldest markers when newer are present" in {
    // given
    implicit val ordLong: Order[AttributedMarkerKey[Timestamp]] = fromOrdering(bySmallestAttributeOrdering)
    val markersInProgressEff = Ref.of[IO, mutable.Set[MarkerKey]](mutable.Set[MarkerKey]())
    val markersByTimestampEff = PQueue.unbounded[IO, AttributedMarkerKey[Timestamp]]
    val markersByOffsetEff = PQueue.unbounded[IO, AttributedMarkerKey[Offset]]

    val markersToRedeliver = for {
      mProg <- markersInProgressEff
      mTst <- markersByTimestampEff
      mOff <- markersByOffsetEff

      mq = new MarkersQueueFs2(0)
      _ <- mq.handleMarker(100, new MarkerKey(1, 1000), new StartMarker(4000L), 10000L) (mProg, mOff, mOff)
      _ <- mq.handleMarker(101, new MarkerKey(1, 1001), new StartMarker(4000L), 12000L)(mProg, mOff, mTst)

      // when
      toRedeliver <- mq.markersToRedeliver(15000)(mTst, mProg)

    } yield toRedeliver

    // then
    markersToRedeliver.asserting(_ should have size 1)
    markersToRedeliver.asserting(_.head.getMessageOffset should be(1000))
  }

  it should "redeliver multiple markers" in {
    // given
    implicit val ordLong: Order[AttributedMarkerKey[Timestamp]] = fromOrdering(bySmallestAttributeOrdering)
    val markersInProgressEff = Ref.of[IO, mutable.Set[MarkerKey]](mutable.Set[MarkerKey]())
    val markersByTimestampEff = PQueue.unbounded[IO, AttributedMarkerKey[Timestamp]]
    val markersByOffsetEff = PQueue.unbounded[IO, AttributedMarkerKey[Offset]]

    val markersToRedeliver = for {
      mProg <- markersInProgressEff
      mTst <- markersByTimestampEff
      mOff <- markersByOffsetEff

      mq = new MarkersQueueFs2(0)
      _ <- mq.handleMarker(100, new MarkerKey(1, 1000), new StartMarker(4000L), 10000L)(mProg, mOff, mOff)
      _ <- mq.handleMarker(101, new MarkerKey(1, 1001), new StartMarker(4000L), 12000L)(mProg, mOff, mOff)

      // when
      toRedeliver <- mq.markersToRedeliver(17000)(mTst, mProg)
    } yield toRedeliver

    // then
    markersToRedeliver.asserting(_ should have size 2)
    markersToRedeliver.asserting(_.map(_.getMessageOffset).toSet should be(Set(1001L, 1000L)))
  }
}

package com.softwaremill.kmq.redelivery

import cats.effect.std.PQueue
import cats.effect.{IO, Ref}
import cats.implicits.toTraverseOps
import com.softwaremill.kmq.redelivery.model.AttributedMarkerKey
import com.softwaremill.kmq.{EndMarker, MarkerKey, MarkerValue, StartMarker}

import scala.collection.mutable

class MarkersQueueFs2(disableRedeliveryBefore: Offset) {

//  implicit private val ordLong: Order[AttributedMarkerKey[Timestamp]] = fromOrdering(bySmallestAttributeOrdering)

//  private val markersInProgressEff = Ref.of[IO, mutable.Set[MarkerKey]](mutable.Set[MarkerKey]())
//  private val markersByTimestampEff = PQueue.unbounded[IO, AttributedMarkerKey[Timestamp]]
//  private val markersByOffsetEff = PQueue.unbounded[IO, AttributedMarkerKey[Offset]]

  private var redeliveryEnabled = false

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue, t: Timestamp)(
    mProgress: Ref[IO, mutable.Set[MarkerKey]],
    mOffset: PQueue[IO, AttributedMarkerKey[Offset]],
    mTst: PQueue[IO, AttributedMarkerKey[Timestamp]]): IO[Unit] = {

    if (markerOffset >= disableRedeliveryBefore) {
      redeliveryEnabled = true
    }

    v match {
      case s: StartMarker =>
        mOffset.offer(AttributedMarkerKey(k, markerOffset)) *>
          mTst.offer(AttributedMarkerKey(k, t + s.getRedeliverAfter)) *>
          mProgress.update(mp => mp + k)

      case _: EndMarker =>
        mProgress.update(mp => mp - k)

      case x => throw new IllegalArgumentException(s"Unknown marker type: ${x.getClass}")
    }
  }

  def markersToRedeliver(now: Timestamp)(
    qMarkersByTimestamp: PQueue[IO, AttributedMarkerKey[Timestamp]],
    markersInProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[List[MarkerKey]] = {

    removeEndedMarkers(qMarkersByTimestamp, markersInProgress)

    if (redeliveryEnabled) {
      val markersToRedeliver: IO[List[MarkerKey]] = for {
        markerToRedeliver <- shouldRedeliverMarkersQueueHead(now)(qMarkersByTimestamp)
        // the first marker, if any, is not ended for sure (b/c of the cleanup that's done at the beginning),
        // but subsequent markers don't have to be.

        // markers
        maybeMarker = markerToRedeliver.map { isThere => isThere.key }
        markersSet <- markersInProgress.get
        maybeKeyInProgress = maybeMarker.flatMap(k => markersSet.find(_ == k))
      } yield (maybeKeyInProgress.toList)
      markersToRedeliver

      // not removing from markersInProgress - until we are sure the message is redelivered (the redeliverer
      // sends an end marker when this is done) - the marker needs to stay for minimum-offset calculations to be
      // correct
    } else {
      IO.pure(List.empty)
    }
  }

  def smallestMarkerOffset_FromActor(
    markersByOffset: PQueue[IO, AttributedMarkerKey[Offset]],
    mProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[Option[Offset]] = {

    removeEndedMarkers(markersByOffset, mProgress = mProgress) *>
      markersByOffset.tryTake.map(maybeMarker => maybeMarker.map(_.attr))
  }

  private def removeEndedMarkers[T](
      queue: PQueue[IO, AttributedMarkerKey[T]],
      mProgress: Ref[IO, mutable.Set[MarkerKey]]
  ): IO[Unit] = {

    for {
      elem <- isHeadEnded(queue, mProgress)
      _ = if (elem) queue.take
    } yield ()
  }

  private def isHeadEnded[T](
    queue: PQueue[IO, AttributedMarkerKey[T]],
    mProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[Boolean] = {

    for {
      opE <- queue.tryTake
      maybeMarker <- opE
        .map { marker =>
          mProgress.get.map(markersInProgress => !markersInProgress.contains(marker.key))
        }
        .traverse(identity)
    } yield (maybeMarker.getOrElse(false))
  }

  private def shouldRedeliverMarkersQueueHead(
      now: Timestamp
  )(markersByTimestamp: PQueue[IO, AttributedMarkerKey[Timestamp]]): IO[Option[AttributedMarkerKey[Timestamp]]] = {

    markersByTimestamp.tryTake.map {
      case sm@Some(marker) if now >= marker.attr => sm
      case _ => None
    }
  }
}

package object model {
  case class AttributedMarkerKey[T](key: MarkerKey, attr: T)
}

object utils {
  def bySmallestAttributeOrdering[T: Ordering]: Ordering[AttributedMarkerKey[T]] =
    new Ordering[AttributedMarkerKey[T]] {
      override def compare(x: AttributedMarkerKey[T], y: AttributedMarkerKey[T]): Int = {
        -implicitly[Ordering[T]].compare(x.attr, y.attr)
      }
    }
}

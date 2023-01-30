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

  private var redeliveryEnabled = false // TODO? Unsafe to stay that way

  def handleMarker(markerOffset: Offset, k: MarkerKey, v: MarkerValue, t: Timestamp)(
    mProgress: Ref[IO, mutable.Set[MarkerKey]],
    markersByOffset: PQueue[IO, AttributedMarkerKey[Offset]],
    markersByTimestamp: PQueue[IO, AttributedMarkerKey[Timestamp]]): IO[Unit] = {

    if (markerOffset >= disableRedeliveryBefore) {
      redeliveryEnabled = true
    }

    def queueSize[T](prefix: String, q: PQueue[IO, T]): IO[Unit] = {
      q.size.flatMap { s =>
        IO.println(s"Q $prefix S: ${s}")
      }
    }

    v match {
      case s: StartMarker =>
        markersByOffset.offer(AttributedMarkerKey(k, markerOffset)) *>
          markersByTimestamp.offer(AttributedMarkerKey(k, t + s.getRedeliverAfter)) *>
          mProgress.update(mp => mp + k)

      case _: EndMarker =>
        mProgress.update(mp => mp - k)

      case x => throw new IllegalArgumentException(s"Unknown marker type: ${x.getClass}")
    }
  }

  def markersToRedeliver(now: Timestamp)(
    markersByTimestamp: PQueue[IO, AttributedMarkerKey[Timestamp]],
    markersInProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[List[MarkerKey]] = {

    removeEndedMarkers(markersByTimestamp, markersInProgress) *>
      (if (redeliveryEnabled) {
      val markersToRedeliver: IO[List[MarkerKey]] = for {
        markerToRedeliver <- shouldRedeliverMarkersQueueHead(now)(markersByTimestamp)
        // the first marker, if any, is not ended for sure (b/c of the cleanup that's done at the beginning),
        // but subsequent markers don't have to be.

        // markers
        maybeMarker = markerToRedeliver.map { isThere => isThere.key }
        markersSet <- markersInProgress.get
        maybeKeyInProgress = maybeMarker.flatMap(k => markersSet.find(_ == k))
      } yield (maybeKeyInProgress.toList)
      markersToRedeliver.flatMap { markers =>
        IO.println(s"markersToRedeliver: $markers") *> // FIXME: this is empty!!!
          IO.pure(markers)
      }

      // not removing from markersInProgress - until we are sure the message is redelivered (the redeliverer
      // sends an end marker when this is done) - the marker needs to stay for minimum-offset calculations to be
      // correct
    } else {
      IO.pure(List.empty)
    })
  }

  def smallestMarkerOffset_FromActor(
    markersByOffset: PQueue[IO, AttributedMarkerKey[Offset]],
    mProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[Option[Offset]] = {

    removeEndedMarkers(markersByOffset, mProgress = mProgress) *>
      markersByOffset.tryTake.map(maybeMarker => maybeMarker.map(_.attr)) // TODO: maybe wrong, dequeues!
  }

  private def removeEndedMarkers[T](
      queue: PQueue[IO, AttributedMarkerKey[T]],
      mProgress: Ref[IO, mutable.Set[MarkerKey]]
  ): IO[Unit] = {

    // dequeue -> check if in progress -> (eventually enqueue, exit || --> dequeue again(last_elem_seen))
    def removeMarker[T](lastSeen: Option[AttributedMarkerKey[T]])(
      queue: PQueue[IO, AttributedMarkerKey[T]],
      mProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[Boolean] = { // false when not in progress

      val status = for {
        maybeEndedMarker <- queue.tryTake // but move next when lastSeen
        markersInProgress <- mProgress.get
        inProgress <- maybeEndedMarker.map { marker =>
          markersInProgress.contains(marker.key)
        } match {
          case Some(true) =>
            // back to queue
            queue.offer(maybeEndedMarker.get) *> // fixme: not elegant // maybe build another list and enqueue at the end?
            IO.pure(false)
          case Some(false) => // try next one
            removeMarker(maybeEndedMarker)(queue, mProgress)
          case _ =>
            IO.pure(false)
        }
      } yield (inProgress)

      status
    }

//    for {
//
//      qsize <- queue.size
//      _ <- IO.println(s"removeEndedMarkers: qSize: $qsize")
//      maybeAttrMarker <- queue.tryTake // FIXME: that doesn't iterate on option
//      _ <- IO.println(s"removeEndedMarkers - checking from queue Off: $maybeAttrMarker")
//      markersInProgress <- mProgress.get
//      _ <- IO.println(s"removeEndedMarkers: in progress: $markersInProgress")
//      stillInProgress <- maybeAttrMarker.map { attrMarker =>
//        mProgress.get.map(markersInProgress =>
//          markersInProgress
//            .find(_ == attrMarker.key)
//            .map(_ => attrMarker))
//      }.traverse(identity)
//      _ <- IO.println(s"removeEndedMarkers: inProgress: $stillInProgress")
//      _ <- stillInProgress.flatten.map(attrMarker => queue.offer(attrMarker)).traverse(identity)
//    } yield ()

  }

//  def cleanSynchronize[T](queueByTst: PQueue[IO, AttributedMarkerKey[T]], markersInProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[Option[Marker]] = {
//
//  }

  //  private def isHeadEnded[T](
//    queue: PQueue[IO, AttributedMarkerKey[T]],
//    mProgress: Ref[IO, mutable.Set[MarkerKey]]): IO[Boolean] = {
//
//    for {
//      opE <- queue.tryTake
//      maybeMarker <- opE
//        .map { marker =>
//          mProgress.get.map(markersInProgress => !markersInProgress.contains(marker.key))
//        }
//        .traverse(identity)
//    } yield (maybeMarker.getOrElse(false))
//  }

  private def shouldRedeliverMarkersQueueHead(
      now: Timestamp
  )(markersByTimestamp: PQueue[IO, AttributedMarkerKey[Timestamp]]): IO[Option[AttributedMarkerKey[Timestamp]]] = {

    IO.println("shouldRedeliverMarkersQueueHead") *>
    markersByTimestamp.tryTake.flatMap { // TODO: dequeues when successful
      case sm@Some(marker) if now >= marker.attr =>
        IO.println(s"redeliver, tst: YES $sm") *>
        IO.pure(sm)
      case Some(marker) => // TODO: enqueue, None
        IO.println(s"redeliver, tst: No $marker") *>
        markersByTimestamp.offer(marker) *>
        IO.pure(None)
      case _ =>
        IO.println(s"redeliver, tst: None") *>
        IO.pure(None)
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

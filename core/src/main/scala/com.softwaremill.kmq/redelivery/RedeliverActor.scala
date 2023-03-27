package com.softwaremill.kmq.redelivery

import cats.effect.std.Queue
import cats.effect.{IO, Resource}
import com.softwaremill.kmq.{KafkaClients, KmqConfig, MarkerKey}

import scala.concurrent.duration._
import fs2.Stream
import org.apache.kafka.clients.producer.KafkaProducer
import org.typelevel.log4cats.slf4j.Slf4jLogger

object RedeliverActor {

  private val logger = Slf4jLogger.getLogger[IO]
  def create(
      p: Partition,
      producer: KafkaProducer[Array[Byte], Array[Byte]],
      config: KmqConfig,
      clients: KafkaClients
  ): Resource[IO, RedeliverActor] = {
    val resource = for {
      redeliverer <- Resource.make(
        IO(new RetryingRedeliverer(new DefaultRedeliverer(p, producer, config, clients)))
      )(r => IO(r.close()))
      mailbox <- Resource.eval(Queue.unbounded[IO, RedeliverActorMessage])
      _ <- Resource.eval(logger.info(s"Started redeliver actor for partition $p"))
      _ <- stream(redeliverer, mailbox).compile.drain.background
    } yield new RedeliverActor(mailbox)

    resource.onFinalize(
      logger.info(s"Stopped redeliver actor for partition $p")
    )
  }

  private def stream(redeliverer: Redeliverer, mailbox: Queue[IO, RedeliverActorMessage]) = {

    val receive: (List[MarkerKey], RedeliverActorMessage) => IO[(List[MarkerKey], Unit)] = {
      case (toRedeliver, RedeliverMarkers(m)) =>
        IO.pure((toRedeliver ++ m, ()))
      case (toRedeliver, DoRedeliver) =>
        val hadRedeliveries = toRedeliver.nonEmpty
        IO(redeliverer.redeliver(toRedeliver))
          .as((Nil, ()))
          .guarantee {
            if (hadRedeliveries)
              mailbox.offer(DoRedeliver)
            else
              mailbox.offer(DoRedeliver).delayBy(1.second).start.void
          }
    }

    Stream
      .fromQueueUnterminated(mailbox)
      .evalMapAccumulate(List.empty[MarkerKey])(receive)
  }

}
final class RedeliverActor private (mailbox: Queue[IO, RedeliverActorMessage]) {
  def tell(message: RedeliverActorMessage): IO[Unit] = mailbox.offer(message)

}

sealed trait RedeliverActorMessage

case class RedeliverMarkers(markers: List[MarkerKey]) extends RedeliverActorMessage

case object DoRedeliver extends RedeliverActorMessage

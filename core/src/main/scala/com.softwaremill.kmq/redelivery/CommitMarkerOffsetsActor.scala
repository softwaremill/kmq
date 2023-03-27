package com.softwaremill.kmq.redelivery

import cats.effect.{IO, Resource}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import fs2.Stream
import cats.effect.std.Queue

import org.typelevel.log4cats.slf4j.Slf4jLogger

class CommitMarkerOffsetsActor private (
    mailbox: Queue[IO, CommitMarkerOffsetsActorMessage]
) {
  def tell(commitMessage: CommitMarkerOffsetsActorMessage): IO[Unit] = mailbox.offer(commitMessage)
}

object CommitMarkerOffsetsActor {

  private val logger = Slf4jLogger.getLogger[IO]
  private def stream(
      consumer: KafkaConsumer[Array[Byte], Array[Byte]],
      markerTopic: String,
      mailbox: Queue[IO, CommitMarkerOffsetsActorMessage]
  ) = {

    def commitOffsets(toCommit: Map[Partition, Offset]): IO[Map[Partition, Offset]] = for {
      _ <-
        if (toCommit.nonEmpty) {
          IO.blocking {
            consumer.commitSync(
              toCommit.map { case (partition, offset) =>
                (new TopicPartition(markerTopic, partition), new OffsetAndMetadata(offset))
              }.asJava
            )
          } >> logger.info(s"Committed marker offsets: $toCommit")
        } else IO.unit
    } yield Map.empty

    val receive: (Map[Partition, Offset], CommitMarkerOffsetsActorMessage) => IO[(Map[Partition, Offset], Unit)] = {
      case (toCommit, CommitOffset(p, o)) =>
        // only updating if the current offset is smaller
        if (toCommit.get(p).fold(true)(_ < o))
          IO.pure((toCommit.updated(p, o), ()))
        else
          IO.pure((toCommit, ()))
      case (state, DoCommit) => commitOffsets(state).map((_, ()))
    }

    Stream
      .awakeEvery[IO](1.second)
      .as(DoCommit)
      .merge(Stream.fromQueueUnterminated(mailbox))
      .evalMapAccumulate(Map.empty[Partition, Offset])(receive)
  }

  def create(
      markerTopic: String,
      markerOffsetGroupId: String,
      clients: KafkaClientsResourceHelpers
  ): Resource[IO, CommitMarkerOffsetsActor] = for {
    consumer <- clients.createConsumer(
      markerOffsetGroupId,
      classOf[ByteArrayDeserializer],
      classOf[ByteArrayDeserializer]
    )
    mailbox <- Resource.eval(Queue.unbounded[IO, CommitMarkerOffsetsActorMessage])
    _ <- stream(consumer, markerTopic, mailbox).compile.drain.background
  } yield new CommitMarkerOffsetsActor(mailbox)

}

sealed trait CommitMarkerOffsetsActorMessage
case class CommitOffset(p: Partition, o: Offset) extends CommitMarkerOffsetsActorMessage
case object DoCommit extends CommitMarkerOffsetsActorMessage

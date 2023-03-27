package com.softwaremill.kmq.redelivery

import java.io.Closeable
import cats.effect.IO
import com.softwaremill.kmq.{KafkaClients, KmqConfig}
import org.typelevel.log4cats.slf4j.Slf4jLogger

object RedeliveryActors {

  private val logger = Slf4jLogger.getLogger[IO]
  def start(clients: KafkaClients, config: KmqConfig): Closeable = {

    import cats.effect.unsafe.implicits.global

    val kafkaClients = new KafkaClientsResourceHelpers(clients)

    val system = for {
      (consumeMarkersActor, shutdown) <- ConsumeMarkersActor.create(config, kafkaClients).allocated
      _ <- consumeMarkersActor.tell(DoConsume)
      _ <- logger.info("Started redelivery actors")
    } yield new Closeable {
      override def close(): Unit = shutdown.unsafeRunSync()
    }

    system.unsafeRunSync()
  }
}

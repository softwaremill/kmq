package com.softwaremill.kmq.redelivery

import java.io.Closeable

import akka.actor.{ActorSystem, Props}
import com.softwaremill.kmq.{KafkaClients, KmqConfig}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Await
import scala.concurrent.duration._

object RedeliveryActors extends StrictLogging {
  def start(clients: KafkaClients, config: KmqConfig): Closeable = {
    val system = ActorSystem("kmq-redelivery")

    val consumeMakersActor = system.actorOf(Props(new ConsumeMarkersActor(clients, config)), "consume-markers-actor")
    consumeMakersActor ! DoConsume

    logger.info("Started redelivery actors")

    new Closeable {
      override def close(): Unit = {
        Await.result(system.terminate(), 1.minute)
      }
    }
  }
}

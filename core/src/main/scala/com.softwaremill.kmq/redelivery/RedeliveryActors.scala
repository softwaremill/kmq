package com.softwaremill.kmq.redelivery

import java.io.Closeable

import akka.actor.{ActorSystem, Props}
import com.softwaremill.kmq.{KafkaClients, KmqConfig}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Await
import scala.concurrent.duration._

import scala.collection.JavaConverters._

object RedeliveryActors extends StrictLogging {
  def start(clients: KafkaClients, config: KmqConfig): Closeable = {
    start(clients, config)
  }

  def start(clients: KafkaClients, config: KmqConfig, extraConfig: Option[java.util.Map[String, Object]] = None): Closeable = {
    val system = ActorSystem("kmq-redelivery")

    val consumeMakersActor = system.actorOf(Props(new ConsumeMarkersActor(clients, config, extraConfig)), "consume-markers-actor")
    consumeMakersActor ! DoConsume

    logger.info("Started redelivery actors")

    new Closeable {
      override def close(): Unit = Await.result(system.terminate(), 1.minute)
    }
  }
}

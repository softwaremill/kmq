package com.softwaremill.kmq.redelivery

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

class RedeliverActor(p: Partition, redeliverer: Redeliverer, markersActor: ActorRef) extends Actor with StrictLogging {
  override def preStart(): Unit = {
    scheduleGetMarkersQuery()

    logger.info(s"${self.path} Started redeliver actor for partition $p")
  }

  override def postStop(): Unit = {
    logger.info(s"${self.path} Stopped redeliver actor for partition $p")
  }

  override def receive: Receive = {
    case MarkersToRedeliver(m) => redeliverer.redeliver(m)
  }

  private def scheduleGetMarkersQuery(): Unit = {
    import context.dispatcher
    context.system.scheduler.schedule(1.second, 1.second, markersActor, GetMarkersToRedeliver(p))
  }
}

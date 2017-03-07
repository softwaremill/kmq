package com.softwaremill.kmq.redelivery

import akka.actor.{Actor, ActorRef, Cancellable}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

class RedeliverActor(p: Partition, redeliverer: Redeliverer, markersActor: ActorRef) extends Actor with StrictLogging {
  private var scheduledGetMarkersQuery: Cancellable = _

  override def preStart(): Unit = {
    scheduledGetMarkersQuery = scheduleGetMarkersQuery()

    logger.info(s"${self.path} Started redeliver actor for partition $p")
  }

  override def postStop(): Unit = {
    scheduledGetMarkersQuery.cancel()

    try redeliverer.close()
    catch {
      case e: Exception => logger.error(s"Cannot close redeliver for partition $p", e)
    }
    
    logger.info(s"${self.path} Stopped redeliver actor for partition $p")
  }

  override def receive: Receive = {
    case MarkersToRedeliver(m) => redeliverer.redeliver(m)
  }

  private def scheduleGetMarkersQuery(): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(1.second, 1.second, markersActor, GetMarkersToRedeliver(p))
  }
}

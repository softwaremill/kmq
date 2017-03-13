package com.softwaremill.kmq.redelivery

import akka.actor.Actor
import com.softwaremill.kmq.MarkerKey
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

class RedeliverActor(p: Partition, redeliverer: Redeliverer) extends Actor with StrictLogging {

  private var toRedeliver: List[MarkerKey] = Nil

  import context.dispatcher

  override def preStart(): Unit = {
    logger.info(s"${self.path} Started redeliver actor for partition $p")
  }

  override def postStop(): Unit = {
    try redeliverer.close()
    catch {
      case e: Exception => logger.error(s"Cannot close redeliverer for partition $p", e)
    }
    
    logger.info(s"${self.path} Stopped redeliver actor for partition $p")
  }

  override def receive: Receive = {
    case RedeliverMarkers(m) =>
      toRedeliver ++= m

    case DoRedeliver =>
      try {
        redeliverer.redeliver(toRedeliver)
        toRedeliver = Nil
      } finally context.system.scheduler.scheduleOnce(1.second, self, DoRedeliver)
  }
}
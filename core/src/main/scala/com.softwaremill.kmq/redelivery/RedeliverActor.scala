package com.softwaremill.kmq.redelivery

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

class RedeliverActor(p: Partition, redeliverer: Redeliverer, markersActor: ActorRef) extends Actor with StrictLogging {
  private val MaxRetries = 16

  import context.dispatcher

  override def preStart(): Unit = {
    logger.info(s"${self.path} Started redeliver actor for partition $p")
  }

  override def postStop(): Unit = {
    try redeliverer.close()
    catch {
      case e: Exception => logger.error(s"Cannot close redeliver for partition $p", e)
    }
    
    logger.info(s"${self.path} Stopped redeliver actor for partition $p")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)

    message
        .collect { case m: MarkersToRedeliver => m }
        .foreach {
          case MarkersToRedeliver(m, retryCounter) if retryCounter > MaxRetries =>
            logger.error(s"Cannot redeliver markers: ${m.map(_.key)}, tried $MaxRetries times, giving up")

          case MarkersToRedeliver(m, retryCounter) =>
            logger.info(s"Failed to redeliver markers ${m.map(_.key)}, trying again (retry number $retryCounter)")
            
            // trying to redeliver one-by-one instead of a batch
            m.foreach { marker =>
              self ! MarkersToRedeliver(List(marker), retryCounter + 1)
            }
        }
  }

  override def receive: Receive = {
    case MarkersToRedeliver(m, _) =>
      try redeliverer.redeliver(m)
      finally context.system.scheduler.scheduleOnce(1.second, self, GetMarkersToRedeliver(p))

    case gm: GetMarkersToRedeliver => markersActor ! gm
  }
}

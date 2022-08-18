package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.{ActorSystem, Cancellable}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink}
import com.softwaremill.kmq._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.{Metric, MetricName}

import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}

/**
 * Combines functionality of [[RedeliverySink]] and [[CommitMarkerSink]].
 */
class RedeliveryTrackerStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                              markersTopic: String, maxPartitions: Int)
                             (implicit system: ActorSystem, ec: ExecutionContext,
                              kafkaClients: KafkaClients, kmqConfig: KmqConfig, clock: Clock) extends StrictLogging {

  val cancellables: java.util.Set[Cancellable] = ConcurrentHashMap.newKeySet()

  class MyDrainingControl[T](control: Control, future: Future[T]) extends Control {
    private val drainingControl = DrainingControl(control, future)

    override def stop(): Future[Done] = {
      cancellables.forEach(_.cancel)
      drainingControl.stop()
    }

    def drainAndShutdown()(implicit ec: ExecutionContext): Future[T] = {
      cancellables.forEach(_.cancel)
      drainingControl.drainAndShutdown()
    }

    override def shutdown(): Future[Done] = throw new UnsupportedOperationException()

    override def isShutdown: Future[Done] = drainingControl.isShutdown

    override def metrics: Future[Map[MetricName, Metric]] = drainingControl.metrics
  }

  def run(): MyDrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          val redeliverySink = RedeliverySink(topicPartition.partition)
          val commitMarkerSink = CommitMarkerSink()

          val cancellable = RunnableGraph
            .fromGraph(GraphDSL.createGraph(redeliverySink, commitMarkerSink)(Keep.left) {
              implicit builder => (sink1, sink2) =>
                import GraphDSL.Implicits._
                val broadcast = builder.add(Broadcast[CommittableMessage[MarkerKey, MarkerValue]](2))
                source ~> broadcast
                broadcast.out(0) ~> Flow[CommittableMessage[MarkerKey, MarkerValue]].async ~> sink1
                broadcast.out(1) ~> Flow[CommittableMessage[MarkerKey, MarkerValue]].async ~> sink2
                ClosedShape
            })
            .run()

          cancellables.add(cancellable)

          Future.successful(())
      }
      .toMat(Sink.ignore)((c, f) => new MyDrainingControl(c, f))
      .run()
  }
}

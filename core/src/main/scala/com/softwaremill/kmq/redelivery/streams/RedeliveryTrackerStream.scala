package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.{ActorSystem, Cancellable}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import com.softwaremill.kmq._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.{Metric, MetricName}

import java.time.Clock
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Combines functionality of [[RedeliverySink]] and [[CommitMarkerSink]].
 */
class RedeliveryTrackerStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                              kafkaClients: KafkaClients, kmqConfig: KmqConfig, maxPartitions: Int)
                             (implicit system: ActorSystem, ec: ExecutionContext, clock: Clock) extends StrictLogging {

  def run(): DrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(kmqConfig.getMarkerTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          val redeliverySink = RedeliverySink(kafkaClients, kmqConfig)(topicPartition.partition)
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

          Future.successful(cancellable)
      }
      .viaMat(Flow.fromGraph(new AggregatingToMatFlow()))(ControlWithCancellable.apply)
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }
}

class AggregatingToMatFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Iterable[T]] {
  val in: Inlet[T] = Inlet("in")
  val out: Outlet[T] = Outlet("out")

  override def shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Iterable[T]) = {
    val logic = new GraphStageLogic(shape) {
      private val _values: mutable.Set[T] = mutable.Set()

      def values: Iterable[T] = _values

      setHandlers(in, out, new InHandler with OutHandler {
        override def onPush(): Unit = {
          val v = grab(in)
          _values += v
          push(out, v)
        }

        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

    logic -> logic.values
  }
}

class ControlWithCancellable(control: Consumer.Control, cancellables: Iterable[Cancellable]) extends Control {

  override def stop(): Future[Done] = {
    cancellables.foreach(_.cancel())
    control.stop()
  }

  override def shutdown(): Future[Done] = {
    cancellables.foreach(_.cancel())
    control.shutdown()
  }

  override def isShutdown: Future[Done] =
    control.isShutdown

  override def metrics: Future[Map[MetricName, Metric]] =
    control.metrics
}

object ControlWithCancellable {
  def apply(control: Consumer.Control, cancellables: Iterable[Cancellable]): ControlWithCancellable =
    new ControlWithCancellable(control, cancellables)
}

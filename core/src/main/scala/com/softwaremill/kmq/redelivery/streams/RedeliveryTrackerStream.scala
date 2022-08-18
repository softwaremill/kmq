package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink}
import com.softwaremill.kmq._
import com.typesafe.scalalogging.StrictLogging

import java.time.Clock
import scala.concurrent.{ExecutionContext, Future}

/**
 * Combines functionality of [[RedeliverySink]] and [[CommitMarkerSink]].
 */
class RedeliveryTrackerStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                              markersTopic: String, maxPartitions: Int)
                             (implicit system: ActorSystem, ec: ExecutionContext,
                              kafkaClients: KafkaClients, kmqConfig: KmqConfig, clock: Clock) extends StrictLogging {

  def run(): DrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          val redeliverySink = RedeliverySink(topicPartition.partition)
          val commitMarkerSink = CommitMarkerSink()

          RunnableGraph
            .fromGraph(GraphDSL.createGraph(redeliverySink, commitMarkerSink)(combineFutures) {
              implicit builder => (sink1, sink2) =>
                import GraphDSL.Implicits._
                val broadcast = builder.add(Broadcast[CommittableMessage[MarkerKey, MarkerValue]](2))
                source ~> broadcast
                broadcast.out(0) ~> Flow[CommittableMessage[MarkerKey, MarkerValue]].async ~> sink1
                broadcast.out(1) ~> Flow[CommittableMessage[MarkerKey, MarkerValue]].async ~> sink2
                ClosedShape
            })
          .run()
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }

  private def combineFutures[T](l: Future[T], r: Future[T]): Future[T] = l.flatMap(_ => r)
}
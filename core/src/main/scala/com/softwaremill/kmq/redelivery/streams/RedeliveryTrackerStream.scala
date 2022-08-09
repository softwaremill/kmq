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

import scala.concurrent.{ExecutionContext, Future}

/**
 * Combines functionality of [[RedeliverySink]] and [[CommitMarkerSink]].
 */
class RedeliveryTrackerStream(markerConsumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
                              markersTopic: String, maxPartitions: Int,
                              kafkaClients: KafkaClients, kmqConfig: KmqConfig)
                             (implicit system: ActorSystem, ec: ExecutionContext) extends StrictLogging {

  private val redeliverySinkFactory = new RedeliverySink(markerConsumerSettings, markersTopic, maxPartitions, kafkaClients, kmqConfig)
  private val commitMarkerSinkFactory = new CommitMarkerSink(markerConsumerSettings, markersTopic, maxPartitions)

  def run(): DrainingControl[Done] = {
    Consumer.committablePartitionedSource(markerConsumerSettings, Subscriptions.topics(markersTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>

          val redeliverySink = redeliverySinkFactory.redeliverySink(topicPartition.partition)
          val commitMarkerSink = commitMarkerSinkFactory.commitMarkerSink()

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

  private def combineFutures(l: Future[Done], r: Future[Done]): Future[Done] = {
    Future.sequence(Seq(l, r)).map(_ => Done)
  }
}
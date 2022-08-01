package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka._
import akka.stream.RestartSettings
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink}
import com.softwaremill.kmq.{MarkerKey, MarkerValue, StartMarker}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class StartMarkerStream(msgConsumerSettings: ConsumerSettings[String, String],
                        markerProducerSettings: ProducerSettings[MarkerKey, MarkerValue],
                        queueTopic: String, markersTopic: String, maxPartitions: Int, messageTimeout: Long)
                       (implicit system: ActorSystem) {

  def startMarkerSink(): Sink[CommittableMessage[String, String], Future[Done]] = {
    val committerSettings = CommitterSettings(system)

    Flow[CommittableMessage[String, String]]
      .map { msg: CommittableMessage[String, String] =>
        ProducerMessage.single(
          new ProducerRecord[MarkerKey, MarkerValue](markersTopic, MarkerKey.fromRecord(msg.record), new StartMarker(msg.record.timestamp() + messageTimeout)),
          msg
        )
      }
      .via(Producer.flexiFlow(markerProducerSettings))
      .map(_.passThrough.committableOffset)
      .toMat(Committer.sink(committerSettings))(Keep.right)
  }

  def run(): DrainingControl[Done] = {
    val restartSettings = RestartSettings(minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2)

    Consumer.committablePartitionedSource(msgConsumerSettings, Subscriptions.topics(queueTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          RestartSource.withBackoff(restartSettings)(() => source)
            .toMat(startMarkerSink())(Keep.right)
            .run()
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }
}
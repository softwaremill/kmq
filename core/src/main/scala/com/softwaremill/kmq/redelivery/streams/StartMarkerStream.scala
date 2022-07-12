package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka._
import akka.stream.scaladsl.Sink
import com.softwaremill.kmq.{MarkerKey, MarkerValue, StartMarker}
import org.apache.kafka.clients.producer.ProducerRecord

class StartMarkerStream(consumerSettings: ConsumerSettings[String, String],
                        markerProducerSettings: ProducerSettings[MarkerKey, MarkerValue],
                        queueTopic: String, markersTopic: String, maxPartitions: Int, messageTimeout: Long)
                       (implicit system: ActorSystem) {

  def run(): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics(queueTopic))
      .mapAsyncUnordered(maxPartitions) {
        case (topicPartition, source) =>
          source
            .map { msg: CommittableMessage[String, String] =>
              ProducerMessage.single(
                new ProducerRecord[MarkerKey, MarkerValue](markersTopic, MarkerKey.fromRecord(msg.record), new StartMarker(msg.record.timestamp() + messageTimeout)), //Note: handle redeliverAfter differently - save relative to message time
                msg
              )
            }
            .via(Producer.flexiFlow(markerProducerSettings)) // TODO: handle error on sending a marker; only commit on producer success; don't commit after failed message

            .map(_.passThrough.committableOffset)
            .runWith(Committer.sink(committerSettings))
      }
      .toMat(Sink.ignore)(DrainingControl.apply)
      .run()
  }
}
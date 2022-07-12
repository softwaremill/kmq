package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka._
import com.softwaremill.kmq.{MarkerKey, MarkerValue, StartMarker}
import org.apache.kafka.clients.producer.ProducerRecord

class StartMarkerStream(consumerSettings: ConsumerSettings[String, String],
                        markerProducerSettings: ProducerSettings[MarkerKey, MarkerValue],
                        queueTopic: String, markersTopic: String, maxPartitions: Int, messageTimeout: Long)
                       (implicit system: ActorSystem) {

  def run(): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committableSource(consumerSettings, Subscriptions.topics(queueTopic))
      // TODO: use partitionedSource instead of groupBy
      .groupBy(maxSubstreams = maxPartitions, f = msg => msg.record.partition) // grouped on queue partitions
      .map { msg: CommittableMessage[String, String] =>
        // TODO: handle error on sending a marker
        ProducerMessage.single(
          new ProducerRecord[MarkerKey, MarkerValue](markersTopic, MarkerKey.fromRecord(msg.record), new StartMarker(msg.record.timestamp() + messageTimeout)), //Note: handle redeliverAfter differently - save relative to message time
          msg
        )
      }
      .via(Producer.flexiFlow(markerProducerSettings))
      .map(_.passThrough.committableOffset)
      .mergeSubstreams
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()
  }
}
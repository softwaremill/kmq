package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.Consumer.DrainingControl
import com.softwaremill.kmq.{MarkerKey, MarkerValue, StartMarker}
import org.apache.kafka.clients.producer.ProducerRecord

class StartMarkerStream {

  // simple version, no batch commit
  def run(consumerSettings: ConsumerSettings[MarkerKey, MarkerValue],
          markersTopic: String, maxPartitions: Int, messageTimeout: Long)
         (implicit system: ActorSystem): DrainingControl[Done] = {
    val committerSettings = CommitterSettings(system)

    Consumer.committableSource(consumerSettings, Subscriptions.topics(markersTopic))
      // TODO: use partitionedSource instead of groupBy
      .groupBy(maxSubstreams = maxPartitions, f = msg => msg.record.key.getPartition) // Note: sorted not on msg.record.partition
      .map { msg: CommittableMessage[MarkerKey, MarkerValue] =>
        // TODO: batch
        ProducerMessage.single(
          new ProducerRecord[MarkerKey, MarkerValue](markersTopic, MarkerKey.fromRecord(msg.record), new StartMarker(msg.record.timestamp() + messageTimeout)), //Note: handle redeliverAfter differently - save relative to message time
          msg.committableOffset
        )
      }
      .map(_.passThrough)
      .mergeSubstreams
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()
  }
}
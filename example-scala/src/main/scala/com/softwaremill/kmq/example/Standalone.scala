package com.softwaremill.kmq.example

import java.time.{Clock, Duration}
import java.util.Random

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.softwaremill.kmq._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object StandaloneReactiveClient extends App with StrictLogging {
  import StandaloneConfig._

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val clock = Clock.systemUTC()

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServer)
    .withGroupId(kmqConfig.getMsgConsumerGroupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val markerProducerSettings = ProducerSettings(system,
    new MarkerKey.MarkerKeySerializer(), new MarkerValue.MarkerValueSerializer())
    .withBootstrapServers(bootstrapServer)
    .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)
  val markerProducer = markerProducerSettings.createKafkaProducer()

  val random = new Random()

  Consumer.committableSource(consumerSettings, Subscriptions.topics(kmqConfig.getMsgTopic)) // 1. get messages from topic
    .map { msg =>
      ProducerMessage.Message(
        new ProducerRecord(kmqConfig.getMarkerTopic, MarkerKey.fromRecord(msg.record), new MarkerValue(true, clock.millis + kmqConfig.getMsgTimeout)), msg)
    }
    .via(Producer.flow(markerProducerSettings, markerProducer)) // 2. write the "start" marker
    .map(_.message.passThrough)
    .mapAsync(1) { msg => // 3. commit offsets after the "start" markers are sent
      msg.committableOffset.commitScaladsl().map(_ => msg.record) // this should be batched
    }
    .mapConcat { msg =>
      // 4. process the messages
      if (random.nextInt(10) != 0) {
        logger.info(s"Processing: ${msg.key()}")
        List(msg)
      } else {
        logger.info(s"Dropping: ${msg.key()}")
        Nil
      }
    }
    .map { msg =>
      new ProducerRecord(kmqConfig.getMarkerTopic, MarkerKey.fromRecord(msg), new MarkerValue(false, clock.millis + kmqConfig.getMsgTimeout))
    }
    .to(Producer.plainSink(markerProducerSettings, markerProducer)) // 5. write "end" markers
    .run()

  logger.info("Press any key to exit ...")
  StdIn.readLine()

  Await.result(system.terminate(), 1 minute)
}

object StandaloneSender extends App with StrictLogging {
  import StandaloneConfig._

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val producerSettings = ProducerSettings(system, new StringSerializer(), new StringSerializer())
    .withBootstrapServers(bootstrapServer)

  Source.tick(0 seconds, 100 millis, ()).zip(Source.unfold(0)(x => Some(x+1, x+1))).map(_._2)
    .map(msg => s"message number $msg")
    .take(100)
    .map { msg => logger.info(s"Sending: '$msg'"); msg }
    .map(msg => new ProducerRecord(kmqConfig.getMsgTopic, msg, msg))
    .to(Producer.plainSink(producerSettings))
    .run()

  logger.info("Press any key to exit ...")
  StdIn.readLine()

  Await.result(system.terminate(), 1 minute)
}

object StandaloneTracker extends App with StrictLogging {
  import StandaloneConfig._

  val doClose = RedeliveryTracker.start(new KafkaClients(bootstrapServer), kmqConfig)

  logger.info("Press any key to exit ...")
  StdIn.readLine()

  doClose.close()
}

object StandaloneConfig {
  val bootstrapServer = "localhost:9092"
  val kmqConfig = new KmqConfig("queue", "markers", "kmq_client", "kmq_redelivery", Duration.ofSeconds(10).toMillis)
}
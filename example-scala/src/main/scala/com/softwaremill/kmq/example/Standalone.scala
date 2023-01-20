package com.softwaremill.kmq.example

import cats.effect.{IO, IOApp, Sync}

import java.time.Duration
import scala.io.StdIn
import com.softwaremill.kmq._
import com.typesafe.scalalogging.StrictLogging
import fs2.Stream
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
//import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

//import scala.concurrent.Await
import scala.concurrent.duration._
//import scala.io.StdIn
//import scala.util.chaining.scalaUtilChainingOps

/*
object StandaloneReactiveClient extends App with StrictLogging {
  import StandaloneConfig._

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  import system.dispatcher

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServer)
    .withGroupId(kmqConfig.getMsgConsumerGroupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val markerProducerSettings =
    ProducerSettings(system, new MarkerKey.MarkerKeySerializer(), new MarkerValue.MarkerValueSerializer())
      .withBootstrapServers(bootstrapServer)
      .withProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[ParititionFromMarkerKey].getName)

  val random = new Random()

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics(kmqConfig.getMsgTopic)) // 1. get messages from topic
    .map { msg =>
      ProducerMessage.Message(
        new ProducerRecord[MarkerKey, MarkerValue](
          kmqConfig.getMarkerTopic,
          MarkerKey.fromRecord(msg.record),
          new StartMarker(kmqConfig.getMsgTimeoutMs)
        ),
        msg
      )
    }
    .via(Producer.flexiFlow(markerProducerSettings)) // 2. write the "start" marker
    .map(_.passThrough)
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
      new ProducerRecord[MarkerKey, MarkerValue](
        kmqConfig.getMarkerTopic,
        MarkerKey.fromRecord(msg),
        EndMarker.INSTANCE
      )
    }
    .to(Producer.plainSink(markerProducerSettings)) // 5. write "end" markers
    .run()

  logger.info("Press any key to exit ...")
  StdIn.readLine()

  Await.result(system.terminate(), 1.minute)
}
*/

object StandaloneSender extends IOApp.Simple with StrictLogging {
  import StandaloneConfig._

  implicit def logger[F[_] : Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def run: IO[Unit] = {

    val randPrefix = util.Random.nextInt(9999)

    val ticks = Stream
      .emits[IO, Int](Range(0, 100))
      .map(_ + randPrefix * 10000)
      .meteredStartImmediately(100.millis)

    val producerSettings = ProducerSettings[IO, String, String]
      .withBootstrapServers(bootstrapServer)

    val kafkaStream = ticks
      .map { msg =>
        ProducerRecord(kmqConfig.getMsgTopic, msg.toString, msg.toString)
      }.evalTap { msg =>
        Logger[IO].info(s"record: $msg")
      }.map { record =>
        ProducerRecords.one(record)
      }.through(KafkaProducer.pipe(producerSettings))

    kafkaStream.compile.drain
  }
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
  val kmqConfig =
    new KmqConfig("queue", "markers", "kmq_client", "kmq_marker",
      "kmq_marker_offset", Duration.ofSeconds(10).toMillis, 1000)
}

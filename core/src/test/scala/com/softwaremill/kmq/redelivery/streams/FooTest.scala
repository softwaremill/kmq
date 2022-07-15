package com.softwaremill.kmq.redelivery.streams

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ClosedShape, Materializer}
import akka.testkit.TestKit
import com.softwaremill.kmq.redelivery.infrastructure.KafkaSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.DurationDouble
import scala.concurrent.{ExecutionContext, Future}

@deprecated("Learning tests")
class FooTest extends TestKit(ActorSystem("test-system")) with AnyFlatSpecLike with KafkaSpec with BeforeAndAfterAll with Eventually {

  implicit val materializer: Materializer = akka.stream.Materializer.matFromSystem
  implicit val ec: ExecutionContext = system.dispatcher

  "FooStream" should "merge ticks" in {
    Source
      .tick(0.1.second, 0.1.second, "tick")
      .merge(Source.tick(0.17.second, 0.17.second, "tack"))
      .wireTap(println(_))
      .runWith(TestSink[String]).request(10).expectNextN(10)
  }

  "FooStream" should "broadcast to sinks" in {
    val source = Source.fromIterator(() => Seq(1, 2, 3).iterator)

    val sink1: Sink[Int, Future[Done]] = Flow[Int].map(_ * 2).wireTap(x => println(s"2: $x")).toMat(Sink.ignore)(Keep.right)
    val sink2: Sink[Int, Future[Done]] = Flow[Int].map(_ * 3).wireTap(x => println(s"3: $x")).toMat(Sink.ignore)(Keep.right)

    RunnableGraph
      .fromGraph(GraphDSL.createGraph(sink1, sink2)(Tuple2.apply) {
        implicit builder => (mult2, mult3) =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[Int](2))
          source ~> broadcast
          broadcast.out(0) ~> mult2
          broadcast.out(1) ~> mult3
          ClosedShape
      })
      .run()
  }

  "FooStream" should "async broadcast to sinks" in {
    val source = Source.fromIterator(() => Seq(1, 2, 3).iterator)

    val sink1: Sink[Int, Future[Done]] = Flow[Int].map(_ * 2).wireTap(x => println(s"2: $x")).toMat(Sink.ignore)(Keep.right)
    val sink2: Sink[Int, Future[Done]] = Flow[Int].map(_ * 3).wireTap(x => println(s"3: $x")).toMat(Sink.ignore)(Keep.right)

    RunnableGraph
      .fromGraph(GraphDSL.createGraph(sink1, sink2)(Tuple2.apply) {
        implicit builder => (mult2, mult3) =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[Int](2))
          source ~> broadcast
          broadcast.out(0) ~> Flow[Int].async~> mult2
          broadcast.out(1) ~> Flow[Int].async~> mult3
          ClosedShape
      })
      .run()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

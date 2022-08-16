package com.softwaremill.kmq.redelivery.streams

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable.ArrayBuffer

class PriorityQueueMapTest extends AnyFlatSpec {

  it should "dequeue according to priority" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("3", 3)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    val heads = ArrayBuffer[Int]()
    while (queueMap.headOption.nonEmpty) {
      heads += queueMap.dequeue()
    }

    // then
    heads should contain theSameElementsInOrderAs Seq(1, 2, 3)
  }

  it should "return heads according to priority after dequeue" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("3", 3)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    val heads = ArrayBuffer[Int]()
    while (queueMap.headOption.nonEmpty) {
      heads += queueMap.headOption.get
      queueMap.dequeue()
    }

    // then
    heads should contain theSameElementsInOrderAs Seq(1, 2, 3)
    queueMap.headOption shouldBe None
  }

  it should "return heads according to priority after remove" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("3", 3)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    val heads = ArrayBuffer[Int]()
    Seq("1", "2", "3").foreach { x =>
      heads += queueMap.headOption.get
      queueMap.remove(x)
    }

    // then
    heads should contain theSameElementsInOrderAs Seq(1, 2, 3)
    queueMap.headOption shouldBe None
  }

  it should "ignore put with duplicate key and same value" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    queueMap.put("1", 1)

    val heads = ArrayBuffer[Int]()
    while (queueMap.headOption.nonEmpty) {
      heads += queueMap.dequeue()
    }

    // then
    heads should contain theSameElementsInOrderAs Seq(1, 2)
  }

  it should "restore on remove and put with duplicate key and same value" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    queueMap.remove("1")
    queueMap.put("1", 1)

    val heads = ArrayBuffer[Int]()
    while (queueMap.headOption.nonEmpty) {
      heads += queueMap.dequeue()
    }

    // then
    heads should contain theSameElementsInOrderAs Seq(1, 2)
  }

  it should "throw exception on put with duplicate key and different value" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("3", 3)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    val caught = intercept[Exception] {
      queueMap.put("1", 4)
    }

    // then
    caught shouldBe a[IllegalArgumentException]
  }

  it should "throw exception on remove and put with duplicate key and different value" in {
    // given
    val queueMap = new PriorityQueueMap[String, Int](valueOrdering = Ordering.Int.reverse)
    queueMap.put("3", 3)
    queueMap.put("1", 1)
    queueMap.put("2", 2)

    // when
    queueMap.remove("1")
    val caught = intercept[Exception] {
      queueMap.put("1", 4)
    }

    // then
    caught shouldBe a[IllegalArgumentException]
  }
}

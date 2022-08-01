package com.softwaremill.kmq.redelivery.streams

import scala.collection.mutable

/**
 * Custom Priority Queue allowing for removal of selected elements by given key.
 *
 * @param valueOrdering value ordering
 * @tparam K key type
 * @tparam V value type
 */
class PriorityQueueMap[K, V](private val valueOrdering: Ordering[V]) {
  private val keys = mutable.Set[K]()
  private val values = new mutable.PriorityQueue[(K, V)]()(ord = orderingByTupleElement2(valueOrdering))

  def put(key: K, value: V): Unit = {
    values.enqueue((key, value))
    keys += key
  }

  def remove(key: K): Unit = {
    keys -= key
  }

  def headOption: Option[V] = {
    dequeueRemovedHeads()
    values.headOption.map(_._2)
  }

  def dequeue(): V = {
    dequeueRemovedHeads()
    values.dequeue()._2
  }

  private def dequeueRemovedHeads(): Unit = {
    while (isHeadRemoved) {
      values.dequeue()
    }
  }

  private def isHeadRemoved = {
    values.headOption.exists(e => !keys.contains(e._1))
  }

  private def orderingByTupleElement2(implicit ord: Ordering[V]): Ordering[(K, V)] =
    (x, y) => ord.compare(x._2, y._2)
}

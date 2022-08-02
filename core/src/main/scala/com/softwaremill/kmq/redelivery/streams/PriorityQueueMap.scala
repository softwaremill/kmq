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
  private val keys = mutable.Map[K, V]()
  private val removedKeys = mutable.Map[K, V]()
  private val values = new mutable.PriorityQueue[(K, V)]()(ord = orderingByTupleElement2(valueOrdering))

  def put(key: K, value: V): Unit = {
    assertUniqueValue(key, value, keys)
    assertUniqueValue(key, value, removedKeys)
    values.enqueue((key, value))
    keys.put(key, value)
  }

  def remove(key: K): Unit = {
    keys.remove(key).foreach(value => removedKeys.put(key, value))
  }

  def headOption: Option[V] = {
    dequeueRemovedHeads()
    values.headOption.map(_._2)
  }

  def dequeue(): V = {
    dequeueRemovedHeads()
    doDequeueHead()
  }

  private def dequeueRemovedHeads(): Unit = {
    while (isHeadRemoved) {
      doDequeueHead()
    }
  }

  private def isHeadRemoved = {
    values.headOption.exists(e => !keys.contains(e._1))
  }

  private def doDequeueHead(): V = {
    val head = values.dequeue()
    keys.remove(head._1)
    removedKeys.remove(head._1)
    head._2
  }

  private def orderingByTupleElement2(implicit ord: Ordering[V]): Ordering[(K, V)] =
    (x, y) => ord.compare(x._2, y._2)

  private def assertUniqueValue(key: K, value: V, map: mutable.Map[K, V]): Unit = {
    if (map.get(key).exists(value != _)) throw new IllegalArgumentException("Duplicate key with different value")
  }
}

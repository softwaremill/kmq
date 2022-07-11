package com.softwaremill.kmq.redelivery.streams

/**
 * Custom mutable single-item collection.
 * @tparam V value type
 */
class CustomHolder[V] {
  private var value: Option[V] = None

  def get: Option[V] = value

  def update(newValue: V): Unit = value = Some(newValue)
}

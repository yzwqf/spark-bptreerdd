package org.apache.spark.examples.bplusrdd.bptree

import scala.reflect.ClassTag
import scala.Ordered._


import java.io._

@SerialVersionUID(321L)
class BPlusTreeConfig (
                        val internalWidth: Int = 64,
                        val leafWidth: Int = 64
                      ) extends Serializable

@SerialVersionUID(123L)
class BPlusTree[K : Ordering, V: ClassTag] (
                                             private val config: BPlusTreeConfig,
                                             private val indexedField: String
                                           ) extends Serializable {
  private val _root = new LeafNode[K, V](config.leafWidth, Counter.inc())
  private var root: Node[K, V] = _root
  private val firstLeaf = _root
  private var largest: Option[K] = None

  def get(k: K): Option[V] = root.get(k)

  def put(k: K, v: V): Boolean = {
    if (largest.isEmpty || largest.get <= k) 
      largest = Some(k)
    val status = root.put(k, v)
    root = root.getRoot
    status
  }

  def range(start: K, end: K): Array[Option[V]] =
    root.range(start, end)

  def report: Unit =
    root.report

  def indexedBy(field: String): Boolean = indexedField == field

  // return the first element who == key, if there is no such element, returns the least element > key, if key > the largest element in bptree, returns (, 0)
  def firstEqualTo(key: K): Tuple2[Option[LeafNode[K, V]], Int] = {
    if (largest.isEmpty || largest.get < key)
      (None, 0)

    root.firstEqualTo(key)
  }

  // return the first element who > key, if key > the largest element in bptree, returns (None, 0)
  def firstGreaterThan(key: K): Tuple2[Option[LeafNode[K, V]], Int] = {
    if (largest.isEmpty || largest.get < key)
      (None, 0)

    root.firstGreaterThan(key)
  }

  def firstLeafNode: Tuple2[Option[LeafNode[K, V]], Int] = (Some(firstLeaf), 0)
}

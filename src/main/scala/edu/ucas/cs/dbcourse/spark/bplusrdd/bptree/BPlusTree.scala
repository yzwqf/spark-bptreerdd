package main.scala.edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

class BPlusTreeConfig (
                        val internalWidth: Int = 64,
                        val leafWidth: Int = 64
                      )

class BPlusTree[K : Ordering, V: ClassTag] (
                                             private val config: BPlusTreeConfig,
                                             private val indexedField: String
                                           ) {
  private val _root = new LeafNode[K, V](config.leafWidth, Counter.inc())
  private var root: Node[K, V] = _root
  private val firstLeaf = _root

  def get(k: K): Option[V] = root.get(k)

  def put(k: K, v: V): Boolean = {
    val status = root.put(k, v)
    root = root.getRoot
    status
  }

  def range(start: K, end: K): Array[Option[V]] =
    firstLeaf.range(start, end)

  def report: Unit =
    root.report

  def indexedBy(field: String): Boolean = indexedField == field

  // return the first element who == key, if there is no such element, returns the least element > key, if key > the largest element in bptree, returns (None, 0)
  def firstEqualTo(key: K): Tuple2[Option[LeafNode[K, V]], Int] = root firstEqualTo key
  // return the first element who > key, if key > the largest element in bptree, returns (None, 0)
  def firstGreaterThan(key: K): Tuple2[Option[LeafNode[K, V]], Int] = root firstGreaterThan key
  def firstLeafNode: Tuple2[Option[LeafNode[K, V]], Int] = (Some(firstLeaf), 0)
}

object BPTree {
  def main(args: Array[String]): Unit = {
    val l = new BPlusTree[Int, Int](new BPlusTreeConfig, "")
    println("inserting")
    val data = 1 to args(0).toInt
    val ans = data.fold(0)(_ + _)

    data.foreach(d => {
      l.put(d, d)
    })
    println("build b+tree success!")

    val low:Int = 99928
    val high:Int = 2089
    println("testing BptSeqIerator")
    val iter = new ProxyIterator[Int, Int](l).filter(new FilterFunction( "", _ <= low, BptFilterOperator.GE, low, high))
    //val iter = new ProxyIterator(l).filter(_ <= low)
    var sum = 0
    iter.foreach(d => sum += d)
    println(sum)
  }
}

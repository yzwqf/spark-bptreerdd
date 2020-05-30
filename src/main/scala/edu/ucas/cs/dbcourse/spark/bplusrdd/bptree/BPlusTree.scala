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

  // return the first element who == key, if there is no such element, returns the least element > key
  def firstEqualTo(key: K): Tuple2[Option[LeafNode[K, V]], Int] = root firstEqualTo key
  // return the first element who > key
  def firstGreaterThan(key: K): Tuple2[Option[LeafNode[K, V]], Int] = root firstGreaterThan key
  def firstLeafNode: Tuple2[Option[LeafNode[K, V]], Int] = (Some(firstLeaf), 0)
}

object BPTree {
  def main(args: Array[String]): Unit = {
    val l = new BPlusTree[Int, Int](new BPlusTreeConfig(3, 3), "")
    println("inserting")
    val data = 1 to args(0).toInt
    val ans = data.fold(0)(_ + _)

    data.foreach(d => {
      l.put(d, d)
    })

    println("retrieving")
    var sum = 0
    data.foreach(d => sum += l.get(d).get)
    if (sum == ans) {
      println("put & get test passed")
    } else {
      println("put & get test failed")
    }

    //println("testing range")
    //l.range(1, 10000).foreach(v => {
    //  println(v.get)
    //})

    val lt:Int = 20
    println("testing BptSeqIerator")
    //val iter = new ProxyIterator(l).filter(new FilterFunction( "a", _ <= lt, BptFilterOperator.LE, lt))
    val iter = new ProxyIterator[Int, Int](l).filter(_ % 2 == 0)
    sum = 0
    iter.foreach(d => sum += d)
    println(sum)
    println((1 to lt).sum)
    if (sum == (1 to lt).sum) {
      println("iterator test passed")
    } else {
      println("iterator test failed")
    }
  }
}

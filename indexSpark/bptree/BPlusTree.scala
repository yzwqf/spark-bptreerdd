
package org.apache.spark.examples.indexSpark.bptree

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

object BPTree {
  def main(args: Array[String]): Unit = {
    val l = new BPlusTree[Long, Long](new BPlusTreeConfig, "")

    val data = Iterator(44, 86, 88, 121, 38, 92, 119, 42, 104, 46, 66, 96, 45, 40, 78, 104, 118, 94, 75, 71, 29, 63, 48, 97, 35, 84, 53, 55, 80, 113, 58, 93, 108, 58, 66, 84, 39, 92, 62, 38, 90, 73, 93, 23, 52, 40, 92, 39, 93, 46, 117, 108, 97, 71, 82, 46, 67, 56, 49, 54, 96, 37, 82, 106, 71, 63, 62, 81, 111, 118, 103, 70, 119, 28, 48, 85, 79, 71, 43, 76, 61, 112, 53, 95, 105, 127, 83, 54, 34, 50, 54, 97, 125, 46, 84, 107, 45, 30, 80, 29)
    val t = new BPlusTree[Int, Int](new BPlusTreeConfig(3, 3), "")
    data.foreach(d => {
      //           println( d.isInstanceOf[Int])
      l.put(d, d)
    })

    println( l.range(0.asInstanceOf[Long], 10).length)

//    println("inserting")
//    val data = 1 to args(0).toInt
//    val ans = data.fold(0)(_ + _)
//    var sum = 0
//    var status = false
//
//    data.foreach(d => {
//      status = l.put(d, d)
//      if (!status) {
//        throw new RuntimeException("B+ Tree building failed")
//      }
//    })
//
//    println("B+ Tree has been built")
//    // l.report
//
//    data.foreach(d => {
//      sum += l.get(d).get
//    })
//
//    if (sum == ans) {
//      println("put/get test passed")
//    } else {
//      println(s"put/get test failed, $ans wanted, got $sum")
//    }
//    // l.report
//
//    val arr = l.range(0, args(0).toInt)
//    sum = 0
//    arr.foreach(d => sum += d.get)
//
//    if (sum == ans) {
//      println("range test passed")
//    } else {
//      println(s"range test failed, $ans wanted, got $sum")
//    }
//
//    println("testing BptSeqIerator")
//    var low:Int = 100000
//    var high:Int = 20890
//    val iter = new ProxyIterator[Int, Int](l).filter(new FilterFunction("", _ <= low, BptFilterOperator.GT, low, high))
//    println(iter.sum) // 0
//
//
//    low = 10211
//    val jter = new ProxyIterator[Int, Int](l).filter(new FilterFunction("", _ <= low, BptFilterOperator.CloseRange, low, high))
//    println(jter.sum == (low to high).sum) // true
//
//    var cnt = 0
//    val kter = new ProxyIterator[Int, Int](l).filter(new FilterFunction("", _ <= low, BptFilterOperator.GE, high, high))
//    kter.foreach {x => cnt = cnt + 1}
//    println(cnt == (100000-high)+1)
//
//    println("Serializability test")
//    val fos = new FileOutputStream("serializableTest.tmp")
//    val oos = new ObjectOutputStream(fos)
//    oos.writeObject(l)
//    oos.close
//
//    val fis = new FileInputStream("serializableTest.tmp")
//    val ois = new ObjectInputStream(fis)
//    val t = ois.readObject().asInstanceOf[BPlusTree[Int, Int]]
//
//    println("Deserialiing")
//    sum = 0
//    data.foreach(d => {
//      sum += t.get(d).get
//    })
//    if (sum == ans) {
//      println("Serialization succeeded")
//    } else {
//      println("Serialization failed")
//    }
  }
}

package main.scala.edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

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
    val l = new BPlusTree[Int, Int](new BPlusTreeConfig, "")
    println("inserting")
    val data = 1 to args(0).toInt
    val ans = data.fold(0)(_ + _)
    var sum = 0
    var status = false

    data.foreach(d => {
      status = l.put(d, d)
      if (!status) {
        throw new RuntimeException("B+ Tree building failed")
      }
    })

    println("B+ Tree has been built")
    // l.report

    data.foreach(d => {
      sum += l.get(d).get
    })

    if (sum == ans) {
      println("put/get test passed")
    } else {
      println(s"put/get test failed, $ans wanted, got $sum")
    }
    // l.report

    val arr = l.range(0, args(0).toInt)
    sum = 0
    arr.foreach(d => sum += d.get)
    
    if (sum == ans) {
      println("range test passed")
    } else {
      println(s"range test failed, $ans wanted, got $sum")
    }

    println("testing BptSeqIerator")
    var low:Int = 100000
    var high:Int = 20890
    val iter = new ProxyIterator[Int, Int](l).filter(new FilterFunction("", _ <= low, BptFilterOperator.GT, low, high))
    println(iter.sum) // 0

    
    low = 10211
    val jter = new ProxyIterator[Int, Int](l).filter(new FilterFunction("", _ <= low, BptFilterOperator.CloseRange, low, high))
    println(jter.sum == (low to high).sum) // true
    
    var cnt = 0
    val kter = new ProxyIterator[Int, Int](l).filter(new FilterFunction("", _ <= low, BptFilterOperator.GE, high, high))
    kter.foreach {x => cnt = cnt + 1}
    println(cnt == (100000-high)+1)

    println("Serializability test")
    val fos = new FileOutputStream("serializableTest.tmp")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(l)
    oos.close

    val fis = new FileInputStream("serializableTest.tmp")
    val ois = new ObjectInputStream(fis)
    val t = ois.readObject().asInstanceOf[BPlusTree[Int, Int]]

    println("Deserialiing")
    sum = 0
    data.foreach(d => {
      sum += t.get(d).get
    })
    if (sum == ans) {
      println("Serialization succeeded")
    } else {
      println("Serialization failed")
    }
  }
}

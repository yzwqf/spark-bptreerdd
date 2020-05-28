package edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

class BPlusTreeConfig (
    val internalWidth: Int = 64,
    val leafWidth: Int = 64
)

class BPlusTree[K <: Ordering, V: ClassTag] (
    private val config: BPlusTreeConfig
) {
    private var _root = new LeafNode[K, V](config.leafWidth, Counter.inc())
    private var root: Node[K] = _root
    private var firstLeaf = _root

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
}

/*
object BPTree {
    def main(args: Array[String]): Unit = {
        val l = new BPlusTree[Int, Int](new BPlusTreeConfig(3, 3))
        println("inserting")
        val data = (1 to args(0).toInt)
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

        println("testing range")
        l.range(1, 10000).foreach(v => {
            println(v.get)
        })
    }
}
*/

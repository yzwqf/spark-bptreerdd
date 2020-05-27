package edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

class BPlusTreeConfig (
    val internalWidth: Int = 64,
    val leafWidth: Int = 64
)

class BPlusTree[K: ClassTag, V: ClassTag] (
    private val config: BPlusTreeConfig
) {
    private var _root = new LeafNode[K, V](config.leafWidth, Counter.inc())
    private var root: Node[K] = _root
    private var firstLeaf = _root

    private def complexPut(k: String): Boolean = {
        return false
    }

    private def simplePut(k: String): Boolean = {
        return false
    }

    def get(k: K)(implicit cmp: (K, K) => Int): Option[V] = {
        root match {
            case in: InternalNode[K, V] => in.get(k)
            case lf: LeafNode[K, V] => lf.get(k)
        }

    }

    def put(k: K, v: V)(implicit cmp: (K, K) => Int): Boolean = {
        var status = false
        root match {
            case inter: InternalNode[K, V] => {
                status = inter.put(k, v)
                root = inter.getRoot
            }
            case leaf: LeafNode[K, V] => {
                status = leaf.put(k, v)
                root = leaf.getRoot
            }
        }
        status
    }

    def range(start: K, end: K)(implicit cmp: (K, K) => Int): Array[Option[V]] = {
        firstLeaf.range(start, end)
    }

    def report: Unit = {
        root.report
    }
}

/*
object BPTree {

    // when you instantiate a BPlusTree instance, please offer an implicit
    // cmp function so BPlusTree knows how to compare keys.
    // I was expecting something like Trait Bound in Rust, but Scala
    // seems not to have such feature, so I used this dumb implicit function
    implicit def cmp(lhs: String, rhs: String): Int = lhs.compareTo(rhs)
    implicit def cmp(lhs: Int, rhs: Int): Int = lhs.compareTo(rhs)

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
package edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag
import scala.collection.mutable

object Counter {
    private var ct: Int = 0

    def inc(): Int = {
        val ret = ct
        ct += 1
        ret
    }
}

private abstract class Node[K: ClassTag] (
    private val nodeWidth: Int = 64,
    val nodeId: Int = 0
) {
    protected var keys: Array[Option[K]] = Array.fill[Option[K]](nodeWidth)(None)
    protected var numKeys:  Int = 0
    var parent: Option[Node[K]] = None

    def probe(k: K)(implicit cmp: (K, K) => Int): Int = {
        // this is correct since no holes would appear in values and keys
        keys.indexWhere(key => key match {
            case Some(k_) => cmp(k, k_) < 0
            case None => true
        })
    }



    def printKey: Unit = {
        keys.foreach(k => k match {
            case Some(k) => println(k)
            case None => {}
        })
    }

    def hasSlot: Boolean = {
        for (i <- 0 until keys.size) {
            keys(i) match {
                case Some(_) =>
                case None => return true
            }
        }
        false
    }

    def getKey(pos: Int): Option[K] = keys(pos)

    def containsKey(key: K): Boolean = {
        keys.filter(k => k match {
            case Some(k_) => k_ == key
            case None => false
        }).size != 0
    }

    def report: Unit = {
        println("this is a node")
    }

    def updateParent(p: Option[Node[K]]): Unit = {
        parent = p
    }
}

private case class InternalNode[K: ClassTag, V] (
    private val nodeWidth: Int = 64,
    override val nodeId: Int = 0
) extends Node[K](nodeWidth + 1, nodeId) {

    // one more slot for making shift easier
    // one more slot to fulfill structural requirements of B+ tree
    private var children: Array[Option[Node[K]]] = 
                            Array.fill[Option[Node[K]]](nodeWidth + 2)(None)
    
    override def hasSlot: Boolean = {
        for (i <- 0 until keys.size - 1) {
            keys(i) match {
                case Some(_) =>
                case None => return true
            }
        }
        false
    }

    override def report: Unit = {
        println(s"internal node $nodeId reporting")
        var keysContent = new StringBuffer
        var childrenContent = new StringBuffer
        for (i <- 0 until numKeys) {
            keysContent.append(keys(i).get.toString() + " ")
            children(i) match {
                case Some(c) => c match {
                    case inter: InternalNode[K, V] => childrenContent.append(inter.nodeId.toString() + " ")
                    case leaf: LeafNode[K, V] => childrenContent.append(leaf.nodeId.toString() + " ")
                }
                case None =>
            }
        }

        children(numKeys) match {
            case Some(c) => c match {
                case inter: InternalNode[K, V] => childrenContent.append(inter.nodeId.toString() + " ")
                case leaf: LeafNode[K, V] => childrenContent.append(leaf.nodeId.toString() + " ")
            }
            case None =>
        }
        
        println(s"keys are: ${keysContent.toString()}")
        println(s"children are: ${childrenContent.toString()}")

        children.foreach(ch => ch match {
            case Some(c) => c.report
            case None => return
        })
    }

    def getRoot: Node[K] = {
        parent match {
            case Some(p) => p match {
                case in: InternalNode[K, V] => in.getRoot
                case lf: LeafNode[K, V] => throw new RuntimeException("parent is not supposed to be a LeafNode")
            }
            case None => this
        }
    }

    def get(k: K)(implicit cmp: (K, K) => Int): Option[V] = {
        for (i <- 0 until numKeys) {
            keys(i) match {
                case Some(ky) => {
                    if (cmp(k, ky) < 0) {
                        children(i) match {
                            case Some(ch) => ch match {
                                case inter: InternalNode[K, V] => return inter.get(k)
                                case leaf: LeafNode[K, V] => return leaf.get(k)
                            }
                            case None => throw new RuntimeException("unexpected None in InternalNode#get")
                        }
                    }
                }
                case None => throw new RuntimeException("unexpected None in InternalNode#get")
            }
        }

        children(numKeys) match {
            case Some(ch) => ch match {
                case inter: InternalNode[K, V] => return inter.get(k)
                case leaf: LeafNode[K, V] => return leaf.get(k)
            }
            case None => throw new RuntimeException("unexpected None in InternalNode#get")
        }
    }

    def put(k: K, v: V)(implicit cmp: (K, K) => Int): Boolean = {
        for (i <- 0 until numKeys) {
            keys(i) match {
                case Some(ky) => {
                    if (cmp(k, ky) < 0) {
                        children(i) match {
                            case Some(ch) => ch match {
                                    case inter: InternalNode[K, V] => return inter.put(k, v)
                                    case leaf: LeafNode[K, V] => return leaf.put(k, v)
                                }
                            case None => {
                                println(s"traversing at $i")
                                throw new RuntimeException("unexpected None in InternalNode#put")
                            }
                        }
                    }
                }
                case None => throw new RuntimeException("unexpected None in InternalNode#put")
            }
        }

        children(numKeys) match {
            case Some(ch) => ch match {
                case inter: InternalNode[K, V] => return inter.put(k, v)
                case leaf: LeafNode[K, V] => return leaf.put(k, v)
            }
            // there is not expected be a None, 
            // numKeys ensures every child within the range is Some
            case None => false
        }
    }

    private def isRoot: Boolean = {
        parent match {
            case Some(_) => false
            case None => true
        }
    }

    private def split()(implicit cmp: (K, K) => Int): Boolean = {
        val split = numKeys / 2
        val splitKey = Some(keys(split).get)
        keys(split) = None

        val newInter = new InternalNode[K, V](nodeWidth, Counter.inc())

        newInter.children(0) = Some(children(split + 1).get)
        children(split + 1) = None
        for(i <- split+1 until numKeys) {
            // println(s"moving keys($i) = ${keys(i).get} to newKeys(${i - split - 1})")
            newInter.keys(i - split - 1) = keys(i)
            // println(s"moving children(${i + 1}) = ${children(i + 1).get.nodeId} to newChildren(${i - split})")
            newInter.children(i - split) = children(i + 1)
            newInter.children(i - split).get.updateParent(Some(newInter))
            keys(i) = None
            children(i + 1) = None
        }
        newInter.numKeys = (numKeys - numKeys / 2) - 1 // splitKey is removed
        numKeys /= 2

        val interPair = Tuple2(this, newInter)
        parent match {
            case Some(p) => p match {
                 case inter: InternalNode[K, V] => inter.putChild(splitKey.get, interPair)
                 case _ => throw new RuntimeException("parent is not an InternalNode")
            }
            case None => {
                val newRoot = new InternalNode[K, V](nodeWidth, Counter.inc())
                newRoot.putChild(splitKey.get, interPair)
            }
        }
        true
    }

    private def probeSplitKey(splitKey: K)(implicit cmp: (K, K) => Int): Int = {
        keys.indexWhere(key => key match {
            case Some(k) => cmp(splitKey, k) < 0
            case None => true
        })
    }

    def putChild(splitKey: K, t: Tuple2[Node[K], Node[K]])(implicit cmp: (K, K) => Int): Boolean = {
        if (numKeys == 0) {
            // this is a new internal node
            keys(0) = Some(splitKey)
            numKeys = 1
            children(0) = Some(t._1)
            children(1) = Some(t._2)

            t._1.updateParent(Some(this))
            t._2.updateParent(Some(this))
            return true
        }

        val putPos = probeSplitKey(splitKey)
        // put newLeaf
        children(numKeys+1) = children(numKeys)
        for (i <- (putPos until numKeys).reverse) {
            keys(i+1) = keys(i)
            children(i+1) = children(i)
        }
        keys(putPos) = Some(splitKey)
        children(putPos+1) = Some(t._2)
        numKeys += 1

        t._1.updateParent(Some(this))
        t._2.updateParent(Some(this))

        if (numKeys == keys.size) {
            split
        }
        true
    }

    def assignNewChildren(c: Array[Option[Node[K]]]) = {
        children = c
    }
}

private case class LeafNode[K: ClassTag, V: ClassTag] (
    private val nodeWidth: Int = 64,
    override val nodeId: Int = 0,
    var next: Option[LeafNode[K, V]] = None
) extends Node[K](nodeWidth + 1, nodeId) {

    // one more slot makes shift much simpler
    private var values: Array[Option[V]] = Array.fill[Option[V]](nodeWidth + 1)(None)
    

    override def hasSlot: Boolean = {
        for (i <- 0 until keys.size - 1) {
            keys(i) match {
                case Some(_) =>
                case None => return true
            }
        }
        false
    }


    override def report: Unit = {
        val keysContent = new StringBuffer
        val valuesContent = new StringBuffer
        println(s"leaf node $nodeId reporting")
        for(i <- 0 until numKeys) {
            keysContent.append(keys(i).get + " ")
            valuesContent.append(values(i).get + " ")
        }
        println(s"keys are: ${keysContent.toString()}")
        println(s"values are: ${valuesContent.toString()}")
    }

    def isRoot: Boolean = {
        parent match {
            case Some(_) => false
            case None => true
        }
    }

    def exsits(k: K): Boolean = {
        keys.exists(key => key match {
            case Some(k_) => k_ == k
            case None => false
        })
    }

    def get(k: K)(implicit cmp: (K, K) => Int): Option[V] = {
        for (i <- 0 until numKeys) {
            if (keys(i).get == k)
                return values(i)
        }
        None
    }
    
    def put(k: K, v: V)(implicit cmp: (K, K) => Int): Boolean = {
        if (hasSlot) {
            simplePut(k, v)
        } else {
            complexPut(k, v)
        }
    }

    def getRoot: Node[K] = {
        if (parent.isEmpty)
            return this

        var walk = parent.get
        while(walk.parent.nonEmpty)
            walk = walk.parent.get

        walk
    }

    def range(start: K, end: K)(implicit cmp: (K, K) => Int): Array[Option[V]] = {
        if (cmp(start, end) > 0) {
            throw new RuntimeException("range start is greater than end")
        }

        val seq = (0 until numKeys).filter(i => {
            (cmp(keys(i).get, start) > 0 || cmp(keys(i).get, start) == 0) &&
            (cmp(keys(i).get, end) < 0 || cmp(keys(i).get, end) == 0)
        })

        var ret = new mutable.ArrayBuffer[Option[V]]

        seq.foreach(i => ret += values(i))
        next match {
            case Some(n) => ret.toArray ++ n.range(start, end)
            case None => ret.toArray
        }
    }

    private def simplePut(k: K, v: V)(implicit cmp: (K, K) => Int): Boolean = {
        // not using hasSlot, since this method is intended to be called
        // when a split is required to insert to 'extra' kv pair into 
        // the leaf to be split
        if (numKeys == keys.size) {
            throw new RuntimeException("no more space, complexPut should be called rather than simplePut")
        }
        val key = Some(k)
        val value = Some(v)
        val pos: Int = probe(k)

        for (i <- (pos until numKeys).reverse) {
            keys(i+1) = keys(i)
            values(i+1) = values(i)
        }
        keys(pos) = key
        values(pos) = value

        numKeys += 1
        return true
    }

    /*
     * The basic idea is as follows: (a key only indicates values LESS than the key)
     *     0. make use of the extra slot, insert kv into this slot
     *     1. find splitKey and index of splitKey
     *     2. keys LESS than splitKey remains, splitKey AND keys GREATER than it are shifted to a new array
     *     3. pass this splitKey and new children array to parent
     *     4. parent is to update its parent recursively
     * 
     * there shall be one more key in newLeaf than 'this'
     * 
     */
    private def complexPut(k: K, v: V)(implicit cmp: (K, K) => Int): Boolean = {
        simplePut(k, v)

        val splitPos = keys.size / 2
        val splitKey = keys(splitPos)

        val newLeaf = new LeafNode[K, V](nodeWidth, Counter.inc())
        for (i <- splitPos until keys.size) {
            newLeaf.simplePut(keys(i).get, values(i).get)
            keys(i) = None
            values(i) = None
        }
        newLeaf.updateParent(parent)
        next = Some(newLeaf)
        numKeys /= 2

        val leafPair = Tuple2(this, newLeaf)
        parent match {
            case Some(p) => p match {
                case inter: InternalNode[K, V] => inter.putChild(splitKey.get, leafPair)
                case _ => throw new RuntimeException("parent is not a InternalNode")
            }
            case None => {
                val inter = new InternalNode[K, V](nodeWidth, Counter.inc())
                inter.putChild(splitKey.get, leafPair)
            }
        }
        true
    }
}

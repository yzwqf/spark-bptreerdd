package main.scala.edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

class BptSeqIerator[K : Ordering, V : ClassTag](
                                                 nextNodeAndPos: Tuple2[Option[LeafNode[K, V]], Int]
                                               ) extends Iterator[V] {
  private var gotNext = false
  private var closed = false
  protected var nextValue: V = _

  protected var nextNode = nextNodeAndPos._1

  protected var pos = nextNode match {
    case Some(_) => nextNodeAndPos._2
    case _ => -1
  }

  protected var finished = nextNode match {
    case Some(_) => false
    case _ => true
  }

  protected def over: Boolean = {
    finished = true
    false
  }

  def getNext(): Boolean = nextNode match {
    case Some(rn) =>
      rn.values(pos) match {
        case Some(value) => {
          pos = pos + 1
          nextValue = value
          true
        }
        case _ => {
          nextNode = rn.next
          pos = 0
          false
        }
      }
    case _ => over
  }

  def close() {
    nextNode = None
    finished = true
  }

  def closeIfNeeded() {
    if (!closed) {
      closed = true
      close()
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        var flag = getNext
        while (!flag && !finished)
          flag = getNext
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): V = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}

class BptPredIerator[K : Ordering, V: ClassTag] (
                                                  nextNodeAndPos: Tuple2[Option[LeafNode[K, V]], Int],
                                                  private val pred: K => Boolean
                                                ) extends BptSeqIerator[K, V](nextNodeAndPos) {

  override def getNext(): Boolean = nextNode match {
    case Some(rn) =>
      rn.keys(pos) match {
        case Some(key) => if (pred(key)) {
            val value = rn.values(pos).get
            pos = pos + 1
            nextValue = value
            true
          } else over
        case _ => {
          nextNode = rn.next
          pos = 0
          false
        }
      }
    case _ => over
  }
}

// used for unindexed field
class BptValIerator[K : Ordering, V : ClassTag] (
                         nextNodeAndPos: Tuple2[Option[LeafNode[K, V]], Int],
                         private val pred: V => Boolean
                       ) extends BptSeqIerator[K, V](nextNodeAndPos) {

  override def getNext(): Boolean = {
    while (true) {
      nextNode match {
        case Some(rn) => rn.values(pos) match {
          case Some(v) => {
            pos = pos + 1
            if (pred(v)) {
              nextValue = v
              return true
            }
          }
          case _ => {
            pos = 0
            nextNode = rn.next
          }
        }
        case _ => return over
      }
    }
    over
  }
}


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

  protected def over(): Unit = {
    finished = true
  }

  /**
   * Method for subclasses to implement to provide the next element.
   *
   * If no next element is available, the subclass should set `finished`
   * to `true` and may return any value (it will be ignored).
   *
   * This convention is required because `null` may be a valid value,
   * and using `Option` seems like it might create unnecessary Some/None
   * instances, given some iterators might be called in a tight loop.
   *
   * @return V, or set 'finished' when done
   */
  def getNext(): Unit = nextNode match {
    case Some(rn) =>
      if (pos < rn.nodeWidth - 1) {
        rn.values(pos) match {
          case Some(value) => {
            pos = pos + 1
            nextValue = value
          }
          case _ => over
        }
      } else {
        nextNode = rn.next
        pos = 0
        getNext
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
        getNext
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

  override def getNext(): Unit = nextNode match {
    case Some(rn) =>
      if (pos < rn.nodeWidth - 1) {
        rn.getKey(pos) match {
          case Some(key) if pred(key) => {
            val value = rn.values(pos).get
            pos = pos + 1
            nextValue = value
          }
          case _ => over
        }
      } else {
        nextNode = rn.next
        pos = 0
        getNext
      }
    case _ => over
  }
}

// used for unindexed field
class BptValIerator[K : Ordering, V : ClassTag] (
                         nextNodeAndPos: Tuple2[Option[LeafNode[K, V]], Int],
                         private val pred: V => Boolean
                       ) extends BptSeqIerator[K, V](nextNodeAndPos) {

  override def getNext(): Unit = nextNode match {
    case Some(rn) =>
      if (pos < rn.nodeWidth - 1) {
        val ppos = pos
        pos = pos + 1
        rn.values(ppos) match {
          case Some(v) if pred(v) => { nextValue = v }
          case _ => getNext
        }
      } else {
        nextNode = rn.next
        pos = 0
        getNext
      }
    case _ => over
  }
}


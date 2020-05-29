package edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

class BptSeqIerator[K : Ordering, V : ClassTag](
    protected var nextNodeAndPos: Tuple[Option[LeafNode[K, V]], Int] 
) extends Iterator[V] {

  private var gotNext = false
  private var nextValue: V = _
  private var closed = false

  private var nextNode = nextNodeAndPos._1 

  private var pos = nextNode match {
    case Some(_) => nextNodeAndPos._2 
    case _ => -1
  }

  protected var finished = nextNode match {
    case Some(_) => false
    case _ => true
  }                

  protected def over(): V = {
    finished = true
    _
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
  override def getNext(): V = nextNode match {
    case Some(rn) => 
      if (pos < rn.nodeWidth - 1) {
        rn.values(pos) match {
          case Some(value) => {
              pos = pos + 1
              value
            }
          case _ => over 
        }
      } else {
        nextNode = rn.next 
        getNext
      }
    case _ => over 
  }

  override def close() {
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
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}

class BptPredIerator[K : Ordering, V: ClassTag] (
    var nextNodeAndPos: Tuple[Option[LeafNode[K, V]], Int],
    private val pred: K => Boolean
) extends BptIterator[K, V](nextNodeAndPos) {

  override def getNext(): V = nextNode match {
    case Some(rn) => 
      if (pos < rn.nodeWidth - 1) {
        rn.getKey(pos) match {
          case Some(key) if pred(key) => {
              val value = rn.values(pos).get
              pos = pos + 1
              value
            }
          case _ => over 
        }
      } else {
        nextNode = rn.next 
        getNext
      }
    case _ => over 
  }
}


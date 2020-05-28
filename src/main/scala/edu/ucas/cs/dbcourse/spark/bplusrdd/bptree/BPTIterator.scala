package edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

object BPTFilterOperator extends Enumeration {
  type BPTFilterOperator = Value
  val LT, GT, EQ, LE, GE, CloseRange, OpenRange = Value
}

class BPTIterator[K : Ordering, V: ClassTag](protected val bpTree: BPlusTree[K, V]):
  extends Iterator[V] {

  private var gotNext = false
  private var nextValue: Node[K, V] = _
  private var closed = false
  protected var finished = false

  protected def getNext(): V
  protected def close()

  def closedIfNeeded() {
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
          closedIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): V = {
    if (!hasNext)
      throw new NoSuchElementException("End of stream")
    gotNext = false
    nextValue
  }

  override def filter(f: K => Boolean): BPIterator = {
  }
}

class FilterFunction[K : Ordering](private val f: K => Boolean, op: String)
  extends Function1[K, Boolean] {
}


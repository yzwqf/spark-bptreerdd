package main.scala.edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

// CloseRange means [a, b]
object BptFilterOperator extends Enumeration {
  type Op = Value
  val LT, GT, EQ, LE, GE, CloseRange, NoOp = Value
}

class FilterFunction[K : Ordering, V](
                                    val field: String,
                                    val f: V => Boolean,
                                    val op: BptFilterOperator.Op,
                                    val args: K*
                                  ) extends Function1[V, Boolean] {

  import Ordered._

  def pred: K => Boolean = op match {
      case BptFilterOperator.LT => _ < args(0)
      case BptFilterOperator.LE => _ <= args(0)
      case BptFilterOperator.EQ => _ == args(0)
      case BptFilterOperator.CloseRange => x => x <= args(1)
      case _ => x => true
  }

  // never used
  def apply(dv: V): Boolean = f(dv)
}

class ProxyIterator[K : Ordering, V: ClassTag](
                                                private val bpTree: BPlusTree[K, V]
                                              ) extends Iterator[V] {
  var nextValue: V = _

  override def hasNext = false
  override def next: V = nextValue

  override def filter(f: V => Boolean): Iterator[V] =
    if (f.isInstanceOf[FilterFunction[K, V]]) {
      val fObj = f.asInstanceOf[FilterFunction[K, V]]
      if (bpTree.indexedBy(fObj.field)) fObj.op match {
        case BptFilterOperator.LT => new BptPredIerator(bpTree.firstLeafNode, fObj.pred)
        case BptFilterOperator.GT => new BptSeqIerator(bpTree.firstGreaterThan(fObj args 0))
        case BptFilterOperator.EQ => new BptPredIerator(bpTree.firstEqualTo(fObj args 0), fObj.pred)
        case BptFilterOperator.LE => new BptPredIerator(bpTree.firstLeafNode, fObj.pred)
        case BptFilterOperator.GE => new BptSeqIerator(bpTree.firstEqualTo(fObj args 0))
        case BptFilterOperator.CloseRange => new BptPredIerator(bpTree.firstEqualTo(fObj args 0), fObj.pred)
      } else new BptValIerator(bpTree.firstLeafNode, f)
    } else new BptValIerator(bpTree.firstLeafNode, f)
}

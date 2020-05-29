package edu.ucas.cs.dbcourse.spark.bplusrdd.bptree

import scala.reflect.ClassTag

// CloseRange means [a, b]
object BptFilterOperator extends Enumeration {
  type BptFilterOperator = Value
  val LT, GT, EQ, LE, GE, CloseRange, = Value
}

class FilterFunction[K : Ordering](
    field: String, 
    op: BptFilterOperator, 
    private val args: K*
) extends Function1[K, Boolean] {

  import Ordered._

  def predicator: K => Boolean = op match {
    case LT => _ < args(0) 
    case EQ => _ == args(0)
    case LE => _ <= args(0)
    case CloseRange => _ <= args(1) 
    case _ => x => true
  }

  def args(i: Int) = args(i)

  // never used
  def apply(key: K): Boolean = true
}

class ProxyIterator[K : Ordering, V: ClassTag](
  private val bpTree: BPlusTree[K, V]
) extends Iterator[V] {

  override def filter(f: K => Boolean): Iterator[V] = {
    val fObj = f.asInstanceOf[FilterFunction]
    if bpTree.indexedBy(fObj.field) fObj.op match {
      case LT => new BptPredIerator(bpTree.firstLeaf, fObj.predicator)
      case GT => new BptSeqIerator(bpTree.firstGreaterThan(fObj args 0))
      case EQ => new BptPredIerator(bpTree.firstEqualTo(fObj args 0), fObj.predicator)
      case LE => new BptPredIerator(bpTree.firstLeaf, fObj.predicator)
      case GE => new BptSeqIerator(bpTree.firstEqualTo(fObj args 0))
      case CloseRange => new BptPredIerator(bpTree.firstEqualTo(fObj args 0), fObj.predicator)
    } else 
      new BptSeqIerator(bpTree.firstLeaf)
  }
}

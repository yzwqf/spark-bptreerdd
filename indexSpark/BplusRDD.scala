
package org.apache.spark.examples.indexSpark

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.examples.indexSpark.bptree._


object JsonTool {
  implicit val formats = DefaultFormats

  def parseJson[K : Ordering](s: String, indexedField: String)(implicit m: Manifest[K]): K = {
    var obj = parse(s)
    (indexedField split "\\.") foreach {field => obj = obj \ field}
    obj.extract[K]
  }
}

class BplusRDDPartition[K : Ordering](
  private val indexedField: String
) extends Serializable {
  private val bpTree = new BPlusTree[K, String](new BPlusTreeConfig(), indexedField)

  def buildBplusTree(iter: Iterator[String])(implicit m: Manifest[K]): this.type = {
    iter.foreach (s => bpTree.put(JsonTool.parseJson[K](s, indexedField), s))
    this
  }

  def iterator: Iterator[String] = new ProxyIterator[K, String](bpTree)
}

object BplusRDDPartition {
  def apply[K : Ordering](iter: Iterator[String], indexedField: String)(implicit m: Manifest[K]) =
    new BplusRDDPartition[K](indexedField).buildBplusTree(iter)
}

// filter: BplusRDDPartition.iterator.filter(cleanF)
class BplusRDD[K : Ordering](private val prev: RDD[BplusRDDPartition[K]])
  extends RDD[String](prev.context, List(new OneToOneDependency(prev))) {

  override val partitioner = prev.partitioner
  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[String] = 
    firstParent[BplusRDDPartition[K]].iterator(split, context).next.iterator
}

object BplusRDD {
  def apply[K : Ordering](src: RDD[String], indexedField: String)(implicit m: Manifest[K]) = 
    new BplusRDD(src.mapPartitions[BplusRDDPartition[K]](
      iter => Iterator(BplusRDDPartition[K](iter, indexedField)), preservesPartitioning = true))
}

package org.apache.spark.examples.bplusrdd 


import org.apache.spark._
import org.apache.spark.rdd.{LocalRDDCheckpointData, RDD}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.examples.bplusrdd.bptree._
import org.apache.spark.storage.StorageLevel


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
  private val bpTree = new BPlusTree[K, String](new BPlusTreeConfig(64, 64), indexedField)

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

  override def persist(newLevel: StorageLevel): this.type = {
    prev.persist(newLevel)
    this
  }

  override def persist(): this.type = {
    prev.persist
    this
  }

  override def cache() = persist()

  override def compute(split: Partition, context: TaskContext): Iterator[String] =
    firstParent[BplusRDDPartition[K]].iterator(split, context).next.iterator
}

object BplusRDD {
  def apply[K : Ordering](src: RDD[String], indexedField: String)(implicit m: Manifest[K]) = 
    new BplusRDD(src.mapPartitions[BplusRDDPartition[K]](
      iter => Iterator(BplusRDDPartition[K](iter, indexedField)), preservesPartitioning = true))
}

package edu.ucas.cs.dbcourse.spark.bplusrdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.json4s._
import org.json4s.jackson.JsonMethods._

import edu.ucas.cs.dbcourse.spark.bplusrdd.bptree._

object JsonTool {
  def parseJson[K <: Ordering](s: String, indexedField: String): K = 
    (parse(s) \ indexedField).extract[K]
}

class BplusRDDPartition[K <: Ordering]() {
  private val bpTree = new BPlusTree[K, String](new BPlusTreeConfig())

  def buildBplusTree(iter: Iterator[String]): this.type = {
    iter.foreach (s => bpTree.put(JsonTool.parseJson[K](s, indexedField), s))
    this
  }
}

object BplusRDDPartition {
  def apply[K <: Ordering](iter: Iterator[String], indexedField: String) =
    new BplusRDDPartition().buildBplusTree(iter)
}

class BplusRDD[K <: Ordering](
    private val prev: RDD[BplusRDDPartition[K]], 
    private val indexedField: String): 
  extends RDD[String](prev.context, List(new OneToOneDependency(prev))) {

  override val partitioner = prev.partitioner
  override protected def getPartitions: Array[Partition] = prev.partitions
}

object BplusRDD {
  def apply[K <: Ordering](src: RDD[String], indexedField: String) = 
    new BplusRDD(src.mapPartitions[BplusRDDPartition[K]](
      iter => Iterator(BplusRDDPartition[K](iter, indexedField)), preservesPartitioning = true), indexedField)
}

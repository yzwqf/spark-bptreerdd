package edu.ucas.cs.dbcourse.spark.bplusrdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject

import edu.ucas.cs.dbcourse.spark.bplusrdd.bptree._

class WrappedJsonObj(val robj: Map[String, Any]) extends AnyVal {
  //def get(key: String) = robj get key
  def toString() = robj.toString
  def contains(indexedField: String): Boolean = robj contains indexedField
}

object WrappedJsonObj {
  def parseFull(s: String): Option[WrappedJsonObj] = JSON.parseFull(s) match {
    case Some(robj: Map[String, Any]): Some(new WrappedJsonObj(rObj))
    case _ => None 
  }
}

class BplusRDDPartition[K <: Ordering]() {
  private val bpTree = new BPlusTree[K, String](new BPlusTreeConfig())

  def buildBplusTree(iter: Iterator[String], f: String => K): this.type = {
    iter.foreach {s => 
      WrappedJsonObj.parseFull(s) match {
        case Some(wObj) => (wObj contains indexedField) match {
            case true => bpTree.put(f s, s)
            case false => throw new SparkException("Bad json format, no indexed field: " + wObj.toString) 
          }
        case _ => throw new SparkException("Bad json format, can not parse: " + s) 
      }
    }
    this
  }
}

object BplusRDDPartition {
  def apply[K <: Ordering](iter: Iterator[String], indexedField: String, f: String => K) =
    new BplusRDDPartition().buildBplusTree(iter, f)
}

class BplusRDD[K <: Ordering](
    private val prev: RDD[BplusRDDPartition[K]], 
    private val indexedField: String): 
  extends RDD[String](prev.context, List(new OneToOneDependency(prev))) {

  override val partitioner = prev.partitioner

  override protected def getPartitions: Array[Partition] = prev.partitions
}

object BplusRDD {
  def apply[K <: Ordering](src: RDD[String], indexedField: String, f: String => K = _) = 
    new BplusRDD(src.mapPartitions[BplusRDDPartition[K]](
      iter => Iterator(BplusRDDPartition[K](iter, indexedField, f)), preservesPartitioning = true), indexedField)
}

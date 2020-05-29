
package org.apache.spark.examples.indexSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Test{
  def main(args: Array[String]): Unit = {
    //    val  context = IndexContext.getOr
    val conf = new SparkConf().setAppName("DFS Read Write Test")

    val context = new IndexContext(conf)
    val spark = SparkSession
      .builder
      .sparkContext(context.asInstanceOf[SparkContext])
//      .appName("DFS Read Write Test")
      .getOrCreate()
    val path : String = "hdfs://localhost:9000/hw1-input/README.md"
    val BTreePath : String = "hdfs://localhost:9000/hw1_input/README.md.btree"
    val data : RDD[String] = spark.sparkContext
      .asInstanceOf[IndexContext].IndexFilterFile[Int, Int](path, BTreePath)
    data.collect().foreach(s => println(s))

  }
}
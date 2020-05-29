
package org.apache.spark.examples.indexSpark

import org.apache.hadoop.fs.FileSystem

import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration


class IndexContext(conf : SparkConf) extends SparkContext(conf : SparkConf) {
  def IndexFilterFile[K : Ordering, V: ClassTag](
                       path: String,
                       bplusTreePath: String,
                       minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
//    println("hahahhah")
    hadoopIndexFile[ LongWritable, Text, K, V](path, bplusTreePath,
      classOf[IndexInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  def hadoopIndexFile[K, V, BK : Ordering, BV: ClassTag](path: String,
                            bplusTreePath: String,
                            inputFormatClass: Class[_ <: InputFormat[K, V]],
                            keyClass: Class[K],
                            valueClass: Class[V],
                            minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {

    assertNotStopped()
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new BplusHadoopRDD[K, V, BK, BV](
      this,
      bplusTreePath,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)

  }
}

object IndexContext extends SparkContext {
  def main(args: Array[String]): Unit = {
//    val  context = IndexContext.getOr

    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .getOrCreate()
//    spark.sparkContext.asInstanceOf[IndexContext].IndexFilterFile[Int,Int]("hdfs://localhost:9000/hw1_input/README.md",
//      "hdfs://localhost:9000/hw1_input/README.md.btree")
  }



}
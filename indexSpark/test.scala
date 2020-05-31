
package org.apache.spark.examples.indexSpark

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.indexSpark.bptree._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

import scala.reflect.Manifest
object Test{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DFS Read Write Test")

    val context = new IndexContext(conf)
    val spark = SparkSession
      .builder
      .sparkContext(context.asInstanceOf[SparkContext])
      .getOrCreate()
    val JsonPath : String = "hdfs://localhost:9000/hw1-input/test.json"
//    val path : String = "hdfs://localhost:9000/hw1-input/README.md"
    val BTreePath : String = "hdfs://localhost:9000/hw1-input/README.md.btree"
    val data : RDD[(LongWritable, Text)] = spark.sparkContext
      .asInstanceOf[IndexContext].IndexFilterFile[Long, Long](JsonPath, BTreePath, true, "father.age")
    val tree_RDD = data.asInstanceOf[BplusHadoopRDD[LongWritable, Text, Long, Long]].build_tree()
//
//      tree_RDD.asInstanceOf[BplusMapPartitionsRDD[BPlusTree[Long, Long], (LongWritable, Text)]]
//        .mysaveAsObjectFile(BTreePath, spark.sparkContext)


//    val tree_RDD: RDD[BPlusTree[Int, Int]] = spark.sparkContext.objectFile(BTreePath)

    val filterd_data = tree_RDD.asInstanceOf[BplusMapPartitionsRDD[BPlusTree[Long, Long], (LongWritable, Text)]].
      get_data[Long, Long](spark.sparkContext, JsonPath, 1, 30)
    filterd_data.map(x => x._2.toString()).collect().foreach(x => println(x))


//
//     val a = new_tree_RDD.map(x => x.range(0, 1000).length).collect()
//      val a = tree_RDD.map(x => x.range(0, 1000).length).collect()
//    data.map(s => s._2.toString).collect().foreach(s => println(s))
//    data.asInstanceOf[BplusHadoopRDD[Int, Long, Int, Long]].viewBpTree(0,100)
//    data.saveAsObjecattFile()
//    val JsonPath : String = "hdfs://localhost:9000/hw1-input/test.json"
//    val path = new Path(JsonPath)
//    val spark = SparkSession.builder().appName("load json").getOrCreate()
//    val jsonData = spark.read.json(JsonPath)
//    jsonData.filter(jsonData("father")("age") > 40).show()

//    val fs = HdfsUtils.getFS()
//    val inStream = fs.open(path)
//    var position: Long = 0;
//    var line_data : String = null
//    inStream.seek(position)
//    implicit val formats = DefaultFormats
//    val  d : BufferedReader = new BufferedReader(new  InputStreamReader(inStream) )
//    do {
//     line_data = d.readLine()
//      if (line_data != null) {
//        val key = ( parse(line_data) \ "father" \ "age").extract[Int]
//      }
//    } while (line_data != null)

  }
}

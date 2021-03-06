
package org.apache.spark.examples.indexSpark

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.{InterruptibleIterator, SparkConf, SparkContext}
import org.apache.spark.examples.indexSpark.bptree._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{NextIterator, SerializableConfiguration}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

import scala.reflect.{ClassTag, Manifest}
object Test{
  def get_data[U: ClassTag, BK: Ordering, BV : ClassTag](sc: SparkContext, rdd: RDD[U],
           filePath: String, start: BK, end: BK): RDD[(LongWritable, Text)] = {
    sc.assertNotStopped()
    FileSystem.getLocal(sc.hadoopConfiguration)
    val confBroadcast = sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, filePath)
    new BplusHadoopLoader[U, LongWritable, Text, BK, BV](
      rdd,
      start,
      end,
      confBroadcast,
      Some(setInputPathsFunc),
      classOf[IndexInputFormat],
      classOf[LongWritable],
      classOf[Text],
      sc.defaultMinPartitions).setName(filePath)
  }

  def extractFrom[T](jsonString: String, key: String)
                    (implicit m: Manifest[T]): T = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val s = key.split("\\.")

    var json = parse(jsonString)
    for (i <- 0 until s.length) {
      json = json \ s(i)
    }
    json.extract[T]

  }

  def build_tree[K: ClassTag, V: ClassTag, BK: Ordering, BV: ClassTag]
        (rdd: RDD[(K, V)], key: String)(implicit m: Manifest[BK]): RDD[BPlusTree[BK, BV]] = {
    //    val cleanF = sc.clean(f)
    new BplusMapPartitionsRDD[BPlusTree[BK, BV], (K, V)](rdd,
      (context, pid, iter) => {
        //        println("hahaha")
        var btree : BPlusTree[BK, BV] = new BPlusTree[BK, BV](new BPlusTreeConfig, "");
        while (iter.hasNext) {
          val a = iter.next()
          val BPTreeKey: BK = extractFrom[BK](a._2.toString, key);
          val BPTreeVal = a._1.asInstanceOf[LongWritable].get().asInstanceOf[BV]
          println(BPTreeKey)
          //          println(BPTreeVal)
          btree.put(BPTreeKey, BPTreeVal)
        }
        //        println( btree.range(0.asInstanceOf[BK], 1000.asInstanceOf[BK]).foreach(v => {
        //          println(v.get)
        //        }))
        var new_iter = new NextIterator[BPlusTree[BK, BV]] {
          var numberRead = 0
          val record = btree;
          override def getNext(): BPlusTree[BK, BV] = {
            if (numberRead == 0) {
              finished = false
              numberRead+= 1
              record
            } else {
              finished = true
              null
            }
          }
          override def close(): Unit = {}
        }
        new InterruptibleIterator[BPlusTree[BK, BV]](context, new_iter)

        //        iter.map(cleanF)
      },
      true)
  }
  def graph_extract(s : String): Long = {
    s.split(" ")(0).toLong
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DFS Read Write Test")

    val context = new IndexContext(conf)
    val spark = SparkSession
      .builder
      .sparkContext(context.asInstanceOf[SparkContext])
      .getOrCreate()
//    val JsonPath : String = "hdfs://localhost:9000/hw1-input/test.json"
//    val BTreePath : String = "hdfs://localhost:9000/hw1-input/test.json.btree"
    val JsonPath : String = "hdfs://localhost:9000/hw1-input/test3.json"
    val BTreePath : String = "hdfs://localhost:9000/hw1-input/test3.json.btree"
    val data : RDD[(LongWritable, Text)] = spark.sparkContext
      .asInstanceOf[IndexContext].IndexFilterFile[Long, Long](JsonPath, BTreePath, true, "father.age")
//    val tree_RDD = data.asInstanceOf[BplusHadoopRDD[LongWritable, Text, Long, Long]].build_tree(graph_extract)
    val tree_RDD = data.asInstanceOf[BplusHadoopRDD[LongWritable, Text, Long, Long]].build_tree()
//      tree_RDD.saveAsObjectFile(BTreePath)

    var startTime = System.nanoTime
    var sum1: Long = 0;
//    val tree_RDD: RDD[BPlusTree[Long, Long]] = spark.sparkContext.objectFile(BTreePath).cache()
    val filterd_data = get_data[BPlusTree[Long, Long], Long, Long](spark.sparkContext, tree_RDD, JsonPath, 11, 35)
    sum1 = filterd_data.count()
    var endTime = System.nanoTime
    println("first tree_RDD : " + (endTime-startTime)/1000000d)


    var sum2: Long = 0
    startTime = System.nanoTime
    val new_filterd_data = get_data[BPlusTree[Long, Long], Long, Long](spark.sparkContext, tree_RDD, JsonPath, 11, 35)
    sum2 = new_filterd_data.count()
    endTime = System.nanoTime
    println("second tree_RDD, cached: " + (endTime-startTime)/1000000d)
//
    var sum3: Long = 0
    startTime = System.nanoTime
    val dataSet = spark.sparkContext.textFile(JsonPath)
    sum3 = dataSet.filter(s => {
      val age = JsonTool.parseJson[Int](s, "father.age")
      age >= 11 && age <= 35
    }).count()
    endTime = System.nanoTime
    println("first hadoopRDD, cached: " + (endTime-startTime)/1000000d)


//    var sum3: Long = 0
//    startTime = System.nanoTime
//    val dataSet = spark.sparkContext.textFile(JsonPath)
//    sum3 = dataSet.filter(s => {
//      val age = graph_extract(s)
//      age >= 11 && age <= 35
//    }).count()
//    endTime = System.nanoTime
//    println("first hadoopRDD, cached: " + (endTime-startTime)/1000000d)

    println(sum1 + "," + sum2 + "," + sum3)

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

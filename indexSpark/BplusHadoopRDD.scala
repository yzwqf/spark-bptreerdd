
package org.apache.spark.examples.indexSpark

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.io.LongWritable
import org.apache.spark.examples.indexSpark.bptree.BPlusTreeConfig
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
//import scala.collection.Iterator

import org.apache.hadoop.mapred.{FileSplit, InputFormat, InputSplit, InvalidInputException, JobConf, RecordReader, Reporter}
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.examples.indexSpark.bptree.{BPlusTree, BPlusTreeConfig}
import org.apache.spark.internal.config.{HADOOP_RDD_IGNORE_EMPTY_SPLITS, IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES}
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD, InputFileBlockHolder, MapPartitionsRDD, RDD}
import org.apache.spark.util.{NextIterator, SerializableConfiguration, ShutdownHookManager}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

import scala.reflect.Manifest

private class BplusHadoopRDDPartition[BK: Ordering, BV: ClassTag]
  (rddId: Int, override val index: Int, s: InputSplit)
  extends HadoopPartition(rddId, index, s) {
//  var bTree: BPlusTree[BK, BV] = new BPlusTree[BK, BV](new BPlusTreeConfig(3, 3), "");
  var hadoopPartition : Partition = null
}

class BplusHadoopRDD[K, V, BK : Ordering, BV: ClassTag](
  @transient sc: SparkContext,
  bplusTreePath: String,
  build_tree: Boolean,
  key: String,
  broadcastedConf:
    Broadcast[SerializableConfiguration],
  initLocalJobConfFuncOpt: Option[JobConf => Unit],
  inputFormatClass: Class[_ <: InputFormat[K, V]],
  keyClass: Class[K],
  valueClass: Class[V],
  minPartitions: Int,
  var prev: RDD[BPlusTree[BK, BV]] = null ) (implicit m: Manifest[BK])
  extends HadoopRDD[K, V](sc, broadcastedConf, initLocalJobConfFuncOpt,
    inputFormatClass, keyClass, valueClass, minPartitions) {
  private val createTime = new Date()

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)

  private val ignoreMissingFiles = sparkContext.conf.get(IGNORE_MISSING_FILES)

  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  def extractFrom[T](jsonString: String)(implicit m: Manifest[T]): T = {
    implicit val formats: DefaultFormats.type = DefaultFormats
      val s = key.split("\\.")

      var json = parse(jsonString)
      for (i <- 0 until s.length) {
        json = json \ s(i)
      }
      json.extract[T]

  }

  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new BplusHadoopRDDPartition[BK, BV](id, i, inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(s"${jobConf.get(FileInputFormat.INPUT_DIR)} doesn't exist and no" +
          s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }
   def build_tree (): RDD[BPlusTree[BK, BV]] = withScope {
//    val cleanF = sc.clean(f)
    new BplusMapPartitionsRDD[BPlusTree[BK, BV], (K, V)](this,
      (context, pid, iter) => {
//        println("hahaha")
        var btree : BPlusTree[BK, BV] = new BPlusTree[BK, BV](new BPlusTreeConfig, "");
        while (iter.hasNext) {
          val a = iter.next()
          val BPTreeKey: BK = extractFrom[BK](a._2.toString);
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
      })
  }
  def viewBpTree(start: BK, end: BK) : Unit = withScope {
//    println("here viewBpTree")
//   val results = sc.runJob(this, (iter: Iterator[(K, V)]) => iter.toArray)
//    for (i <- 0 until partitions.length) {
//      val jobConf = getJobConf();
//      val split = partitions(i).asInstanceOf[BplusHadoopRDDPartition[BK, BV]]
//      println(split.inputSplit.value.asInstanceOf[FileSplit].getPath.toString)
//      if (split.bTree != null) {
//        split.bTree.range(start, end).foreach(v => {
//          println(v.get)
//          println(HdfsUtils.get_line(
//            split.inputSplit.value.asInstanceOf[FileSplit].getPath.toString, (v.get).asInstanceOf[Long]))
//        }
//        )
//      } else {
//        println("split.bTree == null")
//      }
//    }
//    val results = sc.runJob(this, (iter: Iterator[(K, V)]) => {
//      while (iter.hasNext) {
//        var a = iter.next()
//        val BPTreeKey: BK = extractFrom[BK](a._2.toString);
//        val BPTreeVal = a._1.asInstanceOf[LongWritable].get().asInstanceOf[BV]
//        println(BPTreeKey)
//        println(BPTreeVal)
//      }
//
//    })
//    map()
//      Array.concat(results: _*)
  }
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {

    val iter = new NextIterator[(K, V)] {
      private val split = theSplit.asInstanceOf[BplusHadoopRDDPartition[BK, BV]]
      logInfo("Input split: " + split.inputSplit)
      private val jobConf = getJobConf()

      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets InputFileBlockHolder for the file block's information
      split.inputSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
        case _: FileSplit | _: CombineFileSplit =>
          Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private var reader: RecordReader[K, V] = null
      private val inputFormat = getInputFormat(jobConf).asInstanceOf[IndexInputFormat]

      HadoopRDD.addLocalConfiguration(
        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)

      reader =
        try {
          if (build_tree) {
            inputFormat.getRecordReader(split.inputSplit.value,
              jobConf, Reporter.NULL).asInstanceOf[RecordReader[K, V]]
          } else {
//            val spark = SparkSession
//              .builder
//              .appName("DFS Read Write Test")
//              .getOrCreate()
//            val tree_RDD: RDD[BPlusTree[BK, BV]] = spark.sparkContext.objectFile(bplusTreePath)
//            val positons = tree_RDD.map(x => x.range(0.asInstanceOf[BK], 50.asInstanceOf[BK]).length).collect()
             val positons = List(10: Long, 20: Long)
            inputFormat.getIndexRecorder(split.inputSplit.value,
              jobConf, Reporter.NULL, positons).asInstanceOf[RecordReader[K, V]]
          }

        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file: ${split.inputSplit}", e)
            finished = true
            null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
            null
        }
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener[Unit] { context =>
        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()
//      val bTree: BPlusTree[BK, BV] = new BPlusTree[BK, BV](new BPlusTreeConfig(3, 3), "")
      implicit val formats = DefaultFormats

      override def getNext(): (K, V) = {
        try {

          finished = !reader.next(key, value)
//          println(key)
//          println(value)
//          println(finished)
//          println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
//          if (build_tree && !finished) {
//            val BPTreeKey: BK = extractFrom[BK](value.toString);
//            val BPTreeVal = key.asInstanceOf[LongWritable].get().asInstanceOf[BV]
//            println(BPTreeKey)
//            println(BPTreeVal)
////            println("11111111111111111111111111111111111111111")
//            if (partitions(split.index).asInstanceOf[BplusHadoopRDDPartition[BK, BV]].bTree == null) {
//              println("split id:")
//              println(split.index)
//              partitions(split.index).asInstanceOf[BplusHadoopRDDPartition[BK, BV]].bTree = new BPlusTree[BK, BV](new BPlusTreeConfig(3, 3), "")
//            }
//
//            partitions(split.index).asInstanceOf[BplusHadoopRDDPartition[BK, BV]]
//              .bTree.put(BPTreeKey, BPTreeVal)
//          }


        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file: ${split.inputSplit}", e)
            finished = true
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
            split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

}
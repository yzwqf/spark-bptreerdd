
package org.apache.spark.examples.indexSpark

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.reflect.ClassTag

import org.apache.hadoop.mapred.{FileSplit, InputFormat, JobConf, RecordReader, Reporter}
import org.apache.hadoop.mapred.lib.CombineFileSplit

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.{HADOOP_RDD_IGNORE_EMPTY_SPLITS, IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES}
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD, InputFileBlockHolder}
import org.apache.spark.util.{NextIterator, SerializableConfiguration, ShutdownHookManager}


class BplusHadoopRDD[K, V, BK : Ordering, BV: ClassTag](
  sc: SparkContext,
  bplusTreePath: String,
  broadcastedConf:
    Broadcast[SerializableConfiguration],
  initLocalJobConfFuncOpt: Option[JobConf => Unit],
  inputFormatClass: Class[_ <: InputFormat[K, V]],
  keyClass: Class[K],
  valueClass: Class[V],
  minPartitions: Int)
  extends  HadoopRDD(sc, broadcastedConf, initLocalJobConfFuncOpt,
    inputFormatClass, keyClass, valueClass, minPartitions ) {
  // used to build JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)

  private val ignoreMissingFiles = sparkContext.conf.get(IGNORE_MISSING_FILES)

  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      private val split = theSplit.asInstanceOf[HadoopPartition]
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
          inputFormat.getIndexRecorder(split.inputSplit.value,
            jobConf, Reporter.NULL, List(90, 110)).asInstanceOf[RecordReader[K, V]]
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

      override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value)
          println(key)
          println(value)
          println(finished)
          println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
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
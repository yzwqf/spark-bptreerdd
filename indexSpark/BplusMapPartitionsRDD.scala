
package org.apache.spark.examples.indexSpark

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.rdd.{DeterministicLevel, MapPartitionsRDD, RDD}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.reflect.ClassTag
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
private[spark] class BplusMapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning, isFromBarrier, isOrderSensitive) {
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  def mymapPartitions[V: ClassTag]( sc: SparkContext,
                                  f: Iterator[U] => Iterator[V],

                                  preservesPartitioning: Boolean = false): RDD[V] = withScope {
    val cleanedF = sc.clean(f)
    new BplusMapPartitionsRDD[V, U](
      this,
      (context: TaskContext, index: Int, iter: Iterator[U]) => cleanedF(iter),
      preservesPartitioning)
  }

  def mysaveAsObjectFile(path: String, sc: SparkContext): Unit = withScope {
    mymapPartitions(sc, iter => iter.grouped(10).map(_.toArray))
      .asInstanceOf[BplusMapPartitionsRDD[Array[U], U]]
      .mymap(sc, x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }


  def mymap[V: ClassTag](sc: SparkContext, f: U => V): RDD[V] = withScope {
    val cleanF = sc.clean(f)
    new BplusMapPartitionsRDD[V, U](this, (context, pid, iter) => iter.map(cleanF))
  }
  def get_data[BK: ClassTag, BV : ClassTag](sc: SparkContext,
             filePath: String, start: BK, end: BK): RDD[(LongWritable, Text)] = {
    sc.assertNotStopped()
    FileSystem.getLocal(sc.hadoopConfiguration)
//    println("successsssssssssssssssssssssssssss")
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, filePath)
    new BplusHadoopLoader[U, LongWritable, Text, BK, BV](
      this,
      start,
      end,
      confBroadcast,
      Some(setInputPathsFunc),
      classOf[IndexInputFormat],
      classOf[LongWritable],
      classOf[Text],
      sc.defaultMinPartitions).setName(filePath)
  }
}
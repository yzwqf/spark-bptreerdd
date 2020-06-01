
package org.apache.spark.examples.indexSpark

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf, SequenceFileOutputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{DeterministicLevel, MapPartitionsRDD, RDD, SequenceFileRDDFunctions}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.reflect.ClassTag
import org.apache.spark.{Partition, SparkContext, TaskContext, WritableFactory}

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

//  def mymapPartitions[V: ClassTag]( sc: SparkContext,
//                                  f: Iterator[U] => Iterator[V],
//
//                                  preservesPartitioning: Boolean = false): RDD[V] = withScope {
//    val cleanedF = sc.clean(f)
//    new BplusMapPartitionsRDD[V, U](
//      this,
//      (context: TaskContext, index: Int, iter: Iterator[U]) => cleanedF(iter),
//      preservesPartitioning)
//  }

//  def mysaveAsObjectFile(path: String, sc: SparkContext): Unit = withScope {
//    var rdd = mymapPartitions(sc, iter => iter.grouped(10).map(_.toArray))
//      .asInstanceOf[BplusMapPartitionsRDD[Array[U], U]]
//      .mymap(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
//      rdd.mysaveAsSequenceFile(path)
//  }



//  implicit def myrddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
//                                                  (implicit kt: ClassTag[K], vt: ClassTag[V],
//                                                   keyWritableFactory: WritableFactory[K],
//                                                   valueWritableFactory: WritableFactory[V])
//  : mySequenceFileRDDFunctions[K, V] = {
//    implicit val keyConverter = keyWritableFactory.convert
//    implicit val valueConverter = valueWritableFactory.convert
//    new mySequenceFileRDDFunctions(rdd,
//      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
//  }



  def mymap[V: ClassTag]( f: U => V): RDD[V] = withScope {
//    val cleanF = sc.clean(f)
    new BplusMapPartitionsRDD[V, U](this, (context, pid, iter) => iter.map(f))
  }
  def get_data[BK: Ordering, BV : ClassTag](sc: SparkContext, rdd: RDD[U],
             filePath: String, start: BK, end: BK): RDD[(LongWritable, Text)] = {
    sc.assertNotStopped()
    FileSystem.getLocal(sc.hadoopConfiguration)
//    println("successsssssssssssssssssssssssssss")
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
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
}

//class mySequenceFileRDDFunctions[K <% Writable: ClassTag, V <% Writable : ClassTag](
//                            self: RDD[(K, V)],
//                            _keyWritableClass: Class[_ <: Writable],
//                            _valueWritableClass: Class[_ <: Writable])
//  extends Logging
//    with Serializable {
//
//  // TODO the context bound (<%) above should be replaced with simple type bound and implicit
//  // conversion but is a breaking change. This should be fixed in Spark 3.x.
//
//  /**
//   * Output the RDD as a Hadoop SequenceFile using the Writable types we infer from the RDD's key
//   * and value types. If the key or value are Writable, then we use their classes directly;
//   * otherwise we map primitive types such as Int and Double to IntWritable, DoubleWritable, etc,
//   * byte arrays to BytesWritable, and Strings to Text. The `path` can be on any Hadoop-supported
//   * file system.
//   */
//  def mysaveAsSequenceFile(
//     path: String,
//     codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
//    def anyToWritable[U <% Writable](u: U): Writable = u
//
//    // TODO We cannot force the return type of `anyToWritable` be same as keyWritableClass and
//    // valueWritableClass at the compile time. To implement that, we need to add type parameters to
//    // SequenceFileRDDFunctions. however, SequenceFileRDDFunctions is a public class so it will be a
//    // breaking change.
//    val convertKey = self.keyClass != _keyWritableClass
//    val convertValue = self.valueClass != _valueWritableClass
//
//    logInfo("Saving as sequence file of type " +
//      s"(${_keyWritableClass.getSimpleName},${_valueWritableClass.getSimpleName})" )
//    val format = classOf[SequenceFileOutputFormat[Writable, Writable]]
//    val jobConf = new JobConf(self.context.hadoopConfiguration)
//    if (!convertKey && !convertValue) {
//      self.saveAsHadoopFile(path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
//    } else if (!convertKey && convertValue) {
//      self.asInstanceOf[BplusMapPartitionsRDD[(K, V), Array[Long]]].mymap(x => (x._1, anyToWritable(x._2))).saveAsHadoopFile(
//        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
//    } else if (convertKey && !convertValue) {
//      self.asInstanceOf[BplusMapPartitionsRDD[(K, V), Array[Long]]].mymap(x => (anyToWritable(x._1), x._2)).saveAsHadoopFile(
//        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
//    } else if (convertKey && convertValue) {
//      self.asInstanceOf[BplusMapPartitionsRDD[(K, V), Array[Long]]].mymap(x => (anyToWritable(x._1), anyToWritable(x._2))).saveAsHadoopFile(
//        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
//    }
//  }
//}

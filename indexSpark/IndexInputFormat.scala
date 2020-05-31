
package org.apache.spark.examples.indexSpark

import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.io
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf, RecordReader, Reporter, TextInputFormat}

class  IndexInputFormat extends TextInputFormat {
  override def getRecordReader(genericSplit: InputSplit, job: JobConf, reporter: Reporter)
  : RecordReader[LongWritable, Text] = super.getRecordReader(genericSplit, job, reporter)

  def getIndexRecorder(genericSplit: InputSplit, job: JobConf,
    reporter: Reporter, Position: List[Long]): RecordReader[LongWritable, Text] = {
    val split : FileSplit = genericSplit.asInstanceOf[FileSplit];
    new LineRecordReaderWithFilter(job, split, Position)
  }
}

class LineRecordReaderWithFilter(job: JobConf, split: FileSplit, filter: List[Long])
                                            extends RecordReader[LongWritable, Text]{
//  val start : Long = split.getStart()
//  val end : Long = start + split.getLength()
  val file : Path = split.getPath()
  val fs : FileSystem = file.getFileSystem(job)
  val LineNeedOffest : List[Long] = filter
  var index : Int = 0
  var inStream : FSDataInputStream = fs.open(file)
  def createKey(): LongWritable = {
    new LongWritable()
  }

  def createValue(): Text = {
    new Text()
  }

  def  next(key: LongWritable, value: Text) : Boolean = this.synchronized {
    if (index < LineNeedOffest.length) {
      key.set(LineNeedOffest.apply(index))
      val line_content = HdfsUtils.get_line(inStream, LineNeedOffest.apply(index))
      value.set(line_content)
      index = index + 1
      return true
    }
    return false
  }

  override def getProgress: Float = this.synchronized {
    index / LineNeedOffest.length
  }

  override def close(): Unit = {
    inStream.close()
  }

  override def getPos: Long = {
    LineNeedOffest.apply(index)
  }

}
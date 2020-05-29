
package org.apache.spark.examples.indexSpark
import java.io.{BufferedReader, IOException, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, FileUtil, Path}
import org.apache.hadoop.io.IOUtils


object HdfsUtils {

  def getFS(): FileSystem = {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000/")
    conf.set("mapred.remote.os", "Linux")
    FileSystem.get(conf)
  }

  def closeFS(fileSystem: FileSystem) {
    if (fileSystem != null) {
      try {
        fileSystem.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }


  ///////////////////////////////////////////////////////////////////////////
  def get_line(hdfsFilePath: String, position: Long): String = {
    val fileSystem = this.getFS()
    val readPath = new Path(hdfsFilePath)
    val inStream = fileSystem.open(readPath)
    try {
      inStream.seek(position);
      val  d : BufferedReader = new BufferedReader(new  InputStreamReader(inStream) )
      d.readLine()
    } catch {
      case e: IOException => e.printStackTrace(); ""
    } finally {
      IOUtils.closeStream(inStream)
    }

  }

  def get_line(inStream: FSDataInputStream, position: Long): String = {
    try {
      inStream.seek(position);
      val  d : BufferedReader = new BufferedReader(new  InputStreamReader(inStream) )
      d.readLine()
    } catch {
      case e: IOException => e.printStackTrace(); ""
    } finally {

    }

  }


  def main(args: Array[String]) {

    val fileSystem = getFS()
    val path = "/hw1-input"

    try {
//      cat("/hw1-input/README.md")
      println(get_line("/hw1-input/README.md", 90));
      //      println("list path:---------" + path)
      //      val fs = fileSystem.listStatus(new Path(path))
      //      val listPath = FileUtil.stat2Paths(fs)
      //      for (p <- listPath) {
      //        println(p)
      //      }
      //      println("----------------------------------------")

      //      val fdis = fileSystem.open(new Path("/user/hdfs"))
      //      IOUtils.copyBytes(fdis, System.out, 1024)


      val fstats = fileSystem.listStatus(new Path(path))
      for (fstat: FileStatus <- fstats) {
        //        println(fstat.isDirectory() ? "directory": "file")
        //        println("Permission:" + fstat.getPermission())
        //        println("Owner:" + fstat.getOwner())
        //        println("Group:" + fstat.getGroup())
        val path = fstat.getPath().toString
        val name = path.substring(path.toString.lastIndexOf("/") + 1)
        println(name)
        //        println("Size:" + fstat.getLen/1024/1024)
        //        println("Replication:" + fstat.getReplication())
        //        println("Block Size:" + fstat.getBlockSize())

        //        println("#############################")
      }

    } catch {
      case ex: IOException => {
        ex.printStackTrace()
        println(ex.getCause)
        println("link err")
      }
    } finally {
      IOUtils.closeStream(fileSystem)
    }


  }
}
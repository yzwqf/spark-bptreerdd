/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples
import org.apache.spark.examples.bplusrdd._
import org.apache.spark.examples.bplusrdd.bptree._

import org.apache.spark.{SparkConf, SparkContext}

object BptLogQuery {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Bpt Log Query")
    val sc = new SparkContext(sparkConf)

    val dataSet = sc.textFile(args(0))
    val bprdd = BplusRDD[Int](dataSet, "age").cache


    var startTime = System.nanoTime
    val cntx = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.CloseRange, 27, 54))..count
    var endTime = System.nanoTime
    println("bplusrdd setup: " + (endTime-startTime)/100000000d + ", dummy" + cntx)

    startTime = System.nanoTime
    val cnt0 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.CloseRange, 27, 54)).count
    endTime = System.nanoTime
    println("closerange bplusrdd, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt1 = dataSet.filter(s => {
      val age = JsonTool.parseJson[Int](s, "age")
      age >= 27 && age <= 54
    }).count
    endTime = System.nanoTime
    println("closerange hadoopRDD, cached: " + (endTime-startTime)/100000000d)
    println(cnt0 + "," + cnt1)


    startTime = System.nanoTime
    val cnt2 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.LT, 27)).count
    endTime = System.nanoTime
    println("LT bplusrdd, pred, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt3 = bprdd.filter(JsonTool.parseJson[Int](_, "age") < 27).count
    endTime = System.nanoTime
    println("LT bplusrdd, seq, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt4 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") < 27).count
    endTime = System.nanoTime
    println("LT hadooprdd, cached: " + (endTime-startTime)/100000000d)
    println(cnt2 + "," + cnt3 + "," + cnt4)


    startTime = System.nanoTime
    val cnt5 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.GT, 49)).count
    endTime = System.nanoTime
    println("GT bplusrdd, pred, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt6 = bprdd.filter(JsonTool.parseJson[Int](_, "age") > 49).count
    endTime = System.nanoTime
    println("GT bplusrdd, seq, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt7 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") > 49).count
    endTime = System.nanoTime
    println("GT hadooprdd, cached: " + (endTime-startTime)/100000000d)
    println(cnt5 + "," + cnt6 + "," + cnt7)


    startTime = System.nanoTime
    val col0 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.EQ, 37)).collect
    val cnt8 = col0.length
    endTime = System.nanoTime
    println("EQ bplusrdd, pred, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt9 = bprdd.filter(JsonTool.parseJson[Int](_, "age") == 37).count
    endTime = System.nanoTime
    println("EQ bplusrdd, seq, cached: " + (endTime-startTime)/100000000d)

    startTime = System.nanoTime
    val cnt10 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") == 37).count
    endTime = System.nanoTime
    println("EQ hadooprdd, cached: " + (endTime-startTime)/100000000d)
    println(cnt8 + "," + cnt9 + "," + cnt10)

    col0.foreach {s => println(s) }

    sc.stop()
  }
}

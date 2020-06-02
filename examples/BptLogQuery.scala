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

    val dataSet = sc.textFile(if (args.length == 1) args(0) else "/home/yzwqf/test.json").cache
    val bprdd = BplusRDD[Int](dataSet, "age").cache

    var startTime = System.nanoTime
    val cntx = bprdd.filter(s => {
      val age = JsonTool.parseJson[Int](s, "age")
      age >= 38 && age <= 61
    }).collect.length
    var endTime = System.nanoTime
    println("bplusrdd setup: " + (endTime-startTime)/1000000000d + ", dummy " + cntx)

    startTime = System.nanoTime
    val cnty = dataSet.collect.length
    endTime = System.nanoTime
    println("hadoop, walk through: " + (endTime - startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt0 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.CloseRange, 38, 61)).collect.length
    endTime = System.nanoTime
    println("closerange bplusrdd, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt1 = dataSet.filter(s => {
      val age = JsonTool.parseJson[Int](s, "age")
      age >= 38 && age <= 61
    }).collect.length
    endTime = System.nanoTime
    println("closerange hadoopRDD, cached: " + (endTime-startTime)/1000000000d)
    println(cnt0 + "," + cnt1)

    startTime = System.nanoTime
    val cnt2 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.LT, 58)).collect.length
    endTime = System.nanoTime
    println("LT bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt4 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") < 58).collect.length
    endTime = System.nanoTime
    println("LT hadooprdd, cached: " + (endTime-startTime)/1000000000d)
    println(cnt2 + "," + cnt4)

    startTime = System.nanoTime
    val cnt5 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.GT, 49)).collect.length
    endTime = System.nanoTime
    println("GT bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt7 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") > 49).collect.length
    endTime = System.nanoTime
    println("GT hadooprdd, cached: " + (endTime-startTime)/1000000000d)
    println(cnt5 + "," + cnt7)

    startTime = System.nanoTime
    val col0 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.EQ, 37)).collect
    val cnt8 = col0.length
    endTime = System.nanoTime
    println("EQ bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt10 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") == 37).collect.length
    endTime = System.nanoTime
    println("EQ hadooprdd, cached: " + (endTime-startTime)/1000000000d)
    println(cnt8 + "," + cnt10)

    startTime = System.nanoTime
    val cnt11 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.LE, 50)).collect.length
    endTime = System.nanoTime
    println("LE bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt12 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") <= 50).collect.length
    endTime = System.nanoTime
    println("LE hadooprdd, cached: " + (endTime-startTime)/1000000000d)
    println(cnt11 + "," + cnt12)

    startTime = System.nanoTime
    val cnt13 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.GE, 50)).collect.length
    endTime = System.nanoTime
    println("GE bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt14 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") >= 50).collect.length
    endTime = System.nanoTime
    println("GE hadooprdd, cached: " + (endTime-startTime)/1000000000d)
    println(cnt13 + "," + cnt14)


    sc.stop()
  }
}

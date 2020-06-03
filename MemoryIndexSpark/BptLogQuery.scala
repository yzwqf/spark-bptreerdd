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

    val dataSet = sc.textFile(if (args.length == 1) args(0) else "/home/yzwqf/test.json")
    val bprdd = BplusRDD[Int](dataSet, "age").cache

    var startTime = System.nanoTime
    //val cntx = dataSet.filter(s => { JsonTool.parseJson[Int](s, "age") <= 90 }).count
    val cntx = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.LT, 0)).count
    var endTime = System.nanoTime
    println("bplusrdd setup: " + (endTime-startTime)/1000000000d + ", dummy " + cntx)

    /*
    startTime = System.nanoTime
    val cnt0 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.CloseRange, 20, 70)).count
    //val cnt0 = dataSet.filter(s => { val age = JsonTool.parseJson[Int](s, "age")
    //  age >= 20 && age <= 70
    //}).count
    endTime = System.nanoTime
    println("closerange bplusrdd, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt2 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.LT, 30)).count
    //val cnt2 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") < 30).count
    endTime = System.nanoTime
    println("LT bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt5 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.GT, 60)).count
    //val cnt5 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") > 60).count
    endTime = System.nanoTime
    println("GT bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt11 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.LE, 30)).count
    //val cnt11 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") <= 30).count
    endTime = System.nanoTime
    println("LE bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)

    startTime = System.nanoTime
    val cnt13 = bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.GE, 60)).count
    //val cnt13 = dataSet.filter(s => JsonTool.parseJson[Int](s, "age") >= 60).count
    endTime = System.nanoTime
    println("GE bplusrdd, pred, cached: " + (endTime-startTime)/1000000000d)
    */
    sc.stop()
  }
}

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

    val bprdd = BplusRDD[Int](dataSet, "age")
    bprdd.filter(new FilterFunction(_ <= "10", "age", BptFilterOperator.CloseRange, 27, 54)).collect().foreach {cont => println(cont)}
    sc.stop()
  }
}

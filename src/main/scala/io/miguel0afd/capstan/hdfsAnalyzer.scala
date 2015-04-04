/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.miguel0afd.capstan

import org.apache.flink.api.scala._
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object hdfsAnalyzer {

  def main(args: Array[String]): Unit = {
    println("---------------------------")
    println("PROOF OF CONCEPT - HDFS ANALYZER")
    println("---------------------------")
    assert(args.length > 1, "Usage: hdfsAnalyzer <path> <name_node>")
    val path = args(0)
    val nameNode = args(1)
    println("Reading file " + path + " from " + nameNode)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val textInputFormat = new TextInputFormat
    val key = classOf[LongWritable]
    val value = classOf[Text]
    val csvText: DataSet[(LongWritable, Text)] = env.readHadoopFile(textInputFormat, key, value, path)

    /*val text = env.readCsvFile[(Int, String, String, String, String)](
      path,
      fieldDelimiter = ",",
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 3, 5, 8))
    */
    val text = csvText.filter(_._1.get() > 0).map(_._2.toString.split(","))

    //val result = text.filter(x => (x._1 > 1299608 && x._4 == "NY"))
    val result = text
    println("RESULT: " + result.count)
    //print(result.collect.foreach( println ))
    println(result)
  }
}

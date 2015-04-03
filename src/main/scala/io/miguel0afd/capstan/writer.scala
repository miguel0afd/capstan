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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

object writer {
  def main(args: Array[String]): Unit = {
    println("-------------------------")
    println("PROOF OF CONCEPT - WRITER")
    println("-------------------------")
    assert(args.length > 0, "Input file is expected")
    val path = args(0)
    //https://data.consumerfinance.gov/api/views/x94z-ydhh/rows.csv
    println("Reading file " + path.substring(path.lastIndexOf("/")+1))
    val env = ExecutionEnvironment.getExecutionEnvironment
    val mapredInputFormat: FileInputFormat[K, V]
    val key: Class[K]
    val value: Class[V]
    val job: Job
    val result = env.createHadoopInput(mapredInputFormat, key, value, job)
  }
}

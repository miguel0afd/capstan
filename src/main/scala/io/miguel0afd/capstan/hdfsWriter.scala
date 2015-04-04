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

import java.io.{FileInputStream, BufferedInputStream, File}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object hdfsWriter {
  def main(args: Array[String]): Unit = {
    println("-------------------------")
    println("PROOF OF CONCEPT - HDFS WRITER")
    println("-------------------------")
    assert(args.length > 1, "Usage: hdfsWriter <path> <name_node>")
    //https://data.consumerfinance.gov/api/views/x94z-ydhh/rows.csv
    val path = args(0)
    val nameNode = args(1)
    println("Writing file " + path.substring(path.lastIndexOf("/")+1) + " on " + nameNode)

    val configuration = new Configuration
    configuration.set("fs.defaultFS", nameNode)
    val fs = FileSystem.get(configuration);
    val os = fs.create(new Path(path))
    val file = new File(path)
    val in = new BufferedInputStream(new FileInputStream(file))
    val b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      os.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    fs.close()
  }
}

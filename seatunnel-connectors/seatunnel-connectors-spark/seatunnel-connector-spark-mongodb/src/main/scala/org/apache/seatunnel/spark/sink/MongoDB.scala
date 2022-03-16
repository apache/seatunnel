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

package org.apache.seatunnel.spark.sink

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class MongoDB extends SparkBatchSink {

  var writeConfig: WriteConfig = _

  val confPrefix = "writeconfig."

  val map = new collection.mutable.HashMap[String, String]

  override def prepare(env: SparkEnvironment): Unit = {
    TypesafeConfigUtils
      .extractSubConfig(config, confPrefix, false)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        map.put(key, value)
      })
    writeConfig = WriteConfig(map)
  }

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "writeconfig.uri", "writeconfig.database", "writeconfig.collection")
  }

  override def output(df: Dataset[Row], env: SparkEnvironment): Unit = {
    MongoSpark.save(df, writeConfig)
  }

}

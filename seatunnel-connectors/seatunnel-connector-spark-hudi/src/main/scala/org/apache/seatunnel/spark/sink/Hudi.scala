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

import scala.collection.JavaConversions._

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

class Hudi extends SparkBatchSink {

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "hoodie.base.path", "hoodie.table.name")
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append"))
    config = config.withFallback(defaultConfig)
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val writer = df.write.format("org.apache.hudi")
    for (e <- config.entrySet()) {
      writer.option(e.getKey, e.getValue.toString)
    }
    writer.mode(config.getString("save_mode"))
      .save(config.getString("hoodie.base.path"))
  }
}

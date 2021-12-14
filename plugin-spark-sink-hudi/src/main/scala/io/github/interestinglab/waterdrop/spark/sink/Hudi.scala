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
package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.config.ConfigFactory
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Hudi extends SparkBatchSink {

  override def checkConfig(): CheckResult = {
    val requiredOptions = Seq("hoodie.base.path", "hoodie.table.name")
    var missingOptions = new StringBuilder
    requiredOptions.map(opt =>
      if (!config.hasPath(opt)) {
        missingOptions.append(opt).append(",")
      }
    )
    missingOptions.isEmpty match {
      case true =>
        new CheckResult(true, "")
      case false =>
        missingOptions = missingOptions.deleteCharAt(missingOptions.length - 1)
        new CheckResult(false, s"please specify [$missingOptions] as non-empty string")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append",
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val writer = df.write.format("org.apache.hudi")
    for ((k: String, v: String) <- config.entrySet()) {
      writer.option(k, v)
    }
    writer.mode(config.getString("save_mode"))
      .save(config.getString("hoodie.base.path"))
  }
}

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

import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import scala.collection.JavaConversions._

class Tidb extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("use_ssl", config.getString("use_ssl"))
    prop.setProperty("isolation_level", config.getString("isolation_level"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))
    prop.setProperty(JDBCOptions.JDBC_BATCH_INSERT_SIZE, config.getString("batch_size"))

    val saveMode = config.getString("save_mode")

    data.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)

  }

  override def checkConfig(): CheckResult = {

    val requiredOptions = List("url", "table", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {

      val saveModeAllowedValues = List("overwrite", "append", "ignore", "error");

      if (!config.hasPath("save_mode") || saveModeAllowedValues.contains(config.getString("save_mode"))) {
        new CheckResult(true, "")
      } else {
        new CheckResult(false, "wrong value of [save_mode], allowed values: " + saveModeAllowedValues.mkString(", "))
      }

    } else {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
          .map { case (option) => "[" + option + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append", // allowed values: overwrite, append, ignore, error
        "use_ssl" -> "false",
        "isolation_level" -> "NONE",
        "batch_size" -> 150
      )
    )
    config = config.withFallback(defaultConfig)
  }
}

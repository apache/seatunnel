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

    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> config.getString("addr"),
      "tidb.password" -> config.getString("password"),
      "tidb.port" -> config.getString("port"),
      "tidb.user" -> config.getString("user"),
      "database" -> config.getString("database"),
      "table" -> config.getString("table")
    )

    data.write
      .format("tidb")
      .mode("append")
      .options(tidbOptions)
      .save()
  }

  override def checkConfig(): CheckResult = {

    val requiredOptions = List("addr", "port", "database", "table", "user", "password");

    val nonExistsOptions = requiredOptions
      .map(optionName => (optionName, config.hasPath(optionName)))
      .filter { p =>
          val (_, exists) = p
          !exists
      }

    if (nonExistsOptions.nonEmpty) {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
          .map { case (option) => "[" + option + "]" }
          .mkString(", ") + " as non-empty string")

    } else {
      new CheckResult(true, "")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
  }
}

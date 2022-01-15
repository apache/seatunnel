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

import org.apache.seatunnel.common.config.CheckConfigUtil.check
import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class Tidb extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {

    val writer = data.write
      .format("tidb")
      .mode("append")
      .option("tidb.addr", config.getString("addr"))
      .option("tidb.password", config.getString("password"))
      .option("tidb.port", config.getString("port"))
      .option("tidb.user", config.getString("user"))
      .option("database", config.getString("database"))
      .option("table", config.getString("table"))

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false)) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        writer.options(optionMap)
      }
      case Failure(exception) => // do nothing
    }

    writer.save()
  }

  override def checkConfig(): CheckResult = {
    check(config, "addr", "port", "database", "table", "user", "password")
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
  }
}

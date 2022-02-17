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
import org.apache.kudu.spark.kudu._
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

class Kudu extends SparkBatchSink {

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "mode" -> "insert"))
    this.config = config.withFallback(defaultConfig)
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "kudu_master", "kudu_table")
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val kuduContext = new KuduContext(
      config.getString("kudu_master"),
      df.sparkSession.sparkContext)

    val table = config.getString("kudu_table")
    config.getString("mode") match {
      case "insert" => kuduContext.insertRows(df, table)
      case "update" => kuduContext.updateRows(df, table)
      case "upsert" => kuduContext.upsertRows(df, table)
      case "insertIgnore" => kuduContext.insertIgnoreRows(df, table)
    }
  }
}

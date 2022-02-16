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

import java.util

import scala.collection.JavaConversions._

import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row}

class Hive extends SparkBatchSink with Logging {

  override def checkConfig(): CheckResult = {
    if (config.hasPath("sql")) {
      CheckResult.success()
    } else {
      CheckConfigUtil.checkAllExists(config, "result_table_name")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {}

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val sparkSession = df.sparkSession
    if (config.hasPath("sql")) {
      val sql = config.getString("sql")
      sparkSession.sql(sql)
    } else {
      val resultTableName = config.getString("result_table_name")
      val sinkFrame = if (config.hasPath("sink_columns")) {
        df.selectExpr(config.getString("sink_columns").split(","): _*)
      } else {
        df
      }
      val frameWriter: DataFrameWriter[Row] = if (config.hasPath("save_mode")) {
        sinkFrame.write.mode(config.getString("save_mode"))
      } else {
        sinkFrame.write
      }
      if (config.hasPath("partition_by")) {
        val partitionList: util.List[String] = config.getStringList("partition_by")
        frameWriter.partitionBy(partitionList: _*).saveAsTable(resultTableName)
      } else {
        frameWriter.saveAsTable(resultTableName)
      }
    }
  }
}

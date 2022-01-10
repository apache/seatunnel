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
    CheckConfigUtil.check(config, "sql", "source_table_name", "result_table_name")
  }

  override def prepare(env: SparkEnvironment): Unit = {}

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val sparkSession = df.sparkSession
    config.hasPath("sql") match {
      case true => {
        val sql = config.getString("sql")
        sparkSession.sql(sql)
      }
      case _ => {
        val sourceTableName = config.getString("source_table_name")
        val resultTableName = config.getString("result_table_name")
        val sinkColumns = config.hasPath("sink_columns") match {
          case true => config.getString("sink_columns")
          case false => "*"
        }
        val sinkFrame = sparkSession.sql(s"select $sinkColumns from $sourceTableName")
        val frameWriter: DataFrameWriter[Row] = config.hasPath("save_mode") match {
          case true => sinkFrame.write.mode(config.getString("save_mode"))
          case _ => sinkFrame.write
        }
        config.hasPath("partition_by") match {
          case true =>
            val partitionList: util.List[String] = config.getStringList("partition_by")
            frameWriter.partitionBy(partitionList: _*).saveAsTable(resultTableName)
          case _ => frameWriter.saveAsTable(resultTableName)
        }
      }
    }
  }
}

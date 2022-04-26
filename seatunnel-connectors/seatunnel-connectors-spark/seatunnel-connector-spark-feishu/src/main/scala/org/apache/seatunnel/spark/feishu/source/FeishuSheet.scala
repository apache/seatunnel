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

package org.apache.seatunnel.spark.feishu.source

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.seatunnel.spark.feishu.{Config, FeishuClient}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType

class FeishuSheet extends SparkBatchSource {
  override def checkConfig(): CheckResult = {
    checkAllExists(config, Config.APP_ID, Config.APP_SECRET, Config.SHEET_TOKEN)
  }

  /**
   * This is a lifecycle method, this method will be executed after Plugin created.
   *
   * @param env environment
   */
  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        Config.TITLE_LINE_NUM -> 1,
        Config.IGNORE_TITLE_LINE -> true,
        Config.RANGE -> "",
        Config.SHEET_NUM -> 1))
    config = config.withFallback(defaultConfig)
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val spark = env.getSparkSession
    val feishuUtil =
      new FeishuClient(config.getString(Config.APP_ID), config.getString(Config.APP_SECRET))

    val (rows: ArrayBuffer[Row], schema: StructType) = feishuUtil.getDataset(
      config.getString(Config.SHEET_TOKEN),
      config.getString(Config.RANGE),
      config.getInt(Config.TITLE_LINE_NUM),
      config.getBoolean(Config.IGNORE_TITLE_LINE),
      config.getInt(Config.SHEET_NUM))

    spark.createDataFrame(rows, schema)
  }
}

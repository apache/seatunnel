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
package org.apache.seatunnel.spark.iceberg.sink

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.{ConfigFactory, ConfigValueType}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.iceberg.Config.{PATH, SAVE_MODE, SAVE_MODE_DEFAULT}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Iceberg extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val writer = data.write.format("iceberg")
    for (e <- config.entrySet()) {
      e.getValue.valueType match {
        case ConfigValueType.NUMBER =>
          writer.option(e.getKey, config.getLong(e.getKey))
        case ConfigValueType.BOOLEAN =>
          writer.option(e.getKey, config.getBoolean(e.getKey))
        case ConfigValueType.STRING =>
          writer.option(e.getKey, config.getString(e.getKey))
      }
    }
    writer.mode(config.getString(SAVE_MODE))
      .save(config.getString(PATH))
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, PATH)
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        SAVE_MODE -> SAVE_MODE_DEFAULT))
    config = config.withFallback(defaultConfig)
  }

  override def getPluginName: String = "Iceberg"
}

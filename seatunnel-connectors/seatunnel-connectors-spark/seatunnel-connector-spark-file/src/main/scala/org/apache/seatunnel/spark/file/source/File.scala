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
package org.apache.seatunnel.spark.file.source

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfigThrowable
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.file.Config._
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class File extends SparkBatchSource {

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        FORMAT -> DEFAULT_FORMAT
      ))
    config = config.withFallback(defaultConfig)
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, PATH)
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val path = config.getString(PATH)
    val format = config.getString(FORMAT)
    val reader = env.getSparkSession.read.format(format)

    Try(extractSubConfigThrowable(config, OPTION_PREFIX, false)) match {
      case Success(options) => options.entrySet().foreach(e => {
          reader.option(e.getKey, String.valueOf(e.getValue.unwrapped()))
        })
      case Failure(_) =>
    }

    format match {
      case TEXT => reader.load(path).withColumnRenamed("value", "raw_message")
      case PARQUET => reader.parquet(path)
      case JSON => reader.json(path)
      case ORC => reader.orc(path)
      case CSV => reader.csv(path)
      case _ => reader.format(format).load(path)
    }
  }

  override def getPluginName: String = "File"
}

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
package org.apache.seatunnel.spark.source

import org.apache.seatunnel.common.config.CheckConfigUtil.check
import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class Cassandra extends SparkBatchSource {

  override def getData(env: SparkEnvironment): Dataset[Row] = {

    val reader = env.getSparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> config.getString("table"),
        "keyspace" -> config.getString("keyspace"),
        "cluster" -> config.getString("cluster"),
        "pushdown" -> config.getString("pushdown")
      ))

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false)) match {

      case Success(options) =>
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader.options(optionMap)
      case Failure(exception) => // do nothing
    }

    reader.load()
  }

  override def checkConfig(): CheckResult = {
    check(config, "table", "keyspace")
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "cluster" -> "default",
        "pushdown" -> "true"))
    config = config.withFallback(defaultConfig)
  }
}

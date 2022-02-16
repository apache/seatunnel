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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.common.config.CheckConfigUtil.{checkAllExists, checkAtLeastOneExists}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{DataFrameReader, Dataset, Row}

class Neo4j extends SparkBatchSource {

  val neo4jConf: mutable.Map[String, String] = mutable.Map()
  val readFormatter: String = "org.neo4j.spark.DataSource"

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val read: DataFrameReader = env.getSparkSession.read
    read.format(readFormatter)
    neo4jConf.foreach(it => {
      read.option(it._1, it._2)
    })
    read.load()

  }

  override def checkConfig(): CheckResult = {
    val checkMustConfigOneOfParams = checkAtLeastOneExists(config, "query", "labels", "relationship")
    val checkMustConfigAllParams = checkAllExists(config, "result_table_name", "url")
    CheckConfigUtil.mergeCheckResults(checkMustConfigAllParams, checkMustConfigOneOfParams)

  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    config.entrySet().asScala.foreach(entry => {
      neo4jConf.put(entry.getKey, config.getString(entry.getKey))
    })
  }
}

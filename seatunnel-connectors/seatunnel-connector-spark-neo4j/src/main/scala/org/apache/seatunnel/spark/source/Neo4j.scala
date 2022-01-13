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

import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{DataFrameReader, Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

class Neo4j extends SparkBatchSource {

  val neo4jConf: mutable.Map[String, String] = mutable.Map()
  val readFormatter: String = "org.neo4j.spark.DataSource"
  val mustConfiguredAll: Array[String] = Array("result_table_name", "url")
  val mustConfiguredOne: Array[String] = Array("query", "labels", "relationship")


  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val read: DataFrameReader = env.getSparkSession.read
    read.format(readFormatter)
    neo4jConf.foreach(it => {
      read.option(it._1, it._2)
    })
    read.load()

  }

  override def checkConfig(): CheckResult = {

    val notConfigMustAllParam: Array[String] = mustConfiguredAll.filter(item => !config.hasPath(item))
    val notConfigMustOneParam: Array[String] = mustConfiguredOne.filter(item => !config.hasPath(item))
    val isConfiguredOne: Boolean = notConfigMustOneParam.size < mustConfiguredOne.size

    if (notConfigMustAllParam.isEmpty && isConfiguredOne) {
      new CheckResult(true, "neo4j config is enough")
    } else {
      val errorMessage: String = s"neo4j config is not enough please check config [${notConfigMustAllParam.mkString(" ")}] " +
        s"${if (!isConfiguredOne) "you must have one of " + mustConfiguredOne.mkString(" ") else ""}"
      new CheckResult(false, errorMessage)
    }

  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    config.entrySet().asScala.foreach(entry => {
      neo4jConf.put(entry.getKey.replaceAll(raw"_", raw"."), config.getString(entry.getKey))
    })
  }
}

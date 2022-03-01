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

import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Hbase extends SparkBatchSource {

  private final val FORMAT_SOURCE = "org.apache.hadoop.hbase.spark"

  override def prepare(env: SparkEnvironment): Unit = {}

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "hbase.zookeeper.quorum", "catalog")
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val hbaseConfig = HBaseConfiguration.create
    val reader = env.getSparkSession.read.format(FORMAT_SOURCE)
      .option("hbase.spark.use.hbasecontext", true)

    for (e <- config.entrySet()) {
      val key = e.getKey
      reader.option(key, config.getString(key))
      hbaseConfig.set(key, config.getString(key))
    }
    new HBaseContext(env.getSparkSession.sparkContext, hbaseConfig)

    reader.load()
  }
}

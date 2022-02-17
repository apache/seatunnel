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

import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory

class Elasticsearch extends SparkBatchSource {

  private val LOGGER = LoggerFactory.getLogger(classOf[Elasticsearch])

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."

  override def prepare(env: SparkEnvironment): Unit = {
    if (TypesafeConfigUtils.hasSubConfig(config, esPrefix)) {
      val esConfig = TypesafeConfigUtils.extractSubConfig(config, esPrefix, false)
      esConfig
        .entrySet()
        .foreach(entry => {
          val key = entry.getKey
          val value = String.valueOf(entry.getValue.unwrapped())
          esCfg += (esPrefix + key -> value)
        })
    }

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))

    LOGGER.info("Input ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      LOGGER.info("\t" + key + " = " + value)
    }
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val index = config.getString("index")

    env.getSparkSession.read
      .format("org.elasticsearch.spark.sql")
      .options(esCfg)
      .load(index)
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "hosts", "index")
  }

}

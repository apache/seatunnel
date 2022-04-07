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
package org.apache.seatunnel.spark.elasticsearch.sink

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.utils.StringTemplate
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.elasticsearch.Config.{HOSTS, INDEX, INDEX_TYPE, INDEX_TIME_FORMAT, DEFAULT_INDEX_TIME_FORMAT, DEFAULT_INDEX, DEFAULT_INDEX_TYPE}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}
import org.elasticsearch.spark.sql._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class Elasticsearch extends SparkBatchSink {

  private val LOGGER = LoggerFactory.getLogger(classOf[Elasticsearch])

  val esPrefix = "es."
  var esCfg: Map[String, String] = Map()

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val index =
      StringTemplate.substitute(config.getString(INDEX), config.getString(INDEX_TIME_FORMAT))
    df.saveToEs(index + "/" + config.getString(INDEX_TYPE), this.esCfg)
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, HOSTS)
  }

  override def prepare(environment: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        INDEX -> DEFAULT_INDEX,
        INDEX_TYPE -> DEFAULT_INDEX_TYPE,
        INDEX_TIME_FORMAT -> DEFAULT_INDEX_TIME_FORMAT))
    config = config.withFallback(defaultConfig)

    config
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey

        if (key.startsWith(esPrefix)) {
          val value = String.valueOf(entry.getValue.unwrapped())
          esCfg += (key -> value)
        }
      })

    esCfg += ("es.nodes" -> config.getStringList(HOSTS).mkString(","))

    LOGGER.info("Output ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      LOGGER.info("\t" + key + " = " + value)
    }
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "ElasticSearch"
}

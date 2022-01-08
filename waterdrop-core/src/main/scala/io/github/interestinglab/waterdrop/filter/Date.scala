/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.{FormatParser, StringTemplate, UnixMSParser, UnixParser, TimestampParser}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Date extends BaseFilter {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("target_field") && config.hasPath("target_time_format") match {
      case true => (true, "")
      case false => (false, "please specify [target_field] and [target_time_format] as string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> RowConstant.ROOT,
        "target_field" -> "datetime",
        "source_time_format" -> "UNIX_MS",
        "target_time_format" -> "yyyy/MM/dd HH:mm:ss",
        "time_zone" -> "", // TODO
        "default_value" -> "${now}",
        "locale" -> "Locale.US" // TODO
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val targetTimeFormat = config.getString("target_time_format")
    val targetField = config.getString("target_field")
    val defaultValue = config.getString("default_value")
    val dateParser = config.getString("source_time_format") match {
      case "UNIX" => new UnixParser(targetTimeFormat)
      case "UNIX_MS" => new UnixMSParser(targetTimeFormat)
      case "TIMESTAMP" => new TimestampParser(targetTimeFormat)
      case sourceTimeFormat: String => new FormatParser(sourceTimeFormat, targetTimeFormat)
    }

    val func = udf((s: String) => {
      val (success, dateTime) = dateParser.parse(s)
      if (success) {
        dateTime
      } else {
        StringTemplate.substitute(defaultValue, targetTimeFormat)
      }
    })

    config.getString("source_field") match {
      case RowConstant.ROOT => df.withColumn(targetField, func(lit(System.currentTimeMillis().toString)))
      case srcField: String => df.withColumn(targetField, func(col(srcField)))
    }
  }
}

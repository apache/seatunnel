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

package org.apache.seatunnel.spark.transform

import scala.collection.JavaConversions._
import scala.util.matching.Regex

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.seatunnel.common.Constants
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.seatunnel.spark.transform.ReplaceConfig._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

class Replace extends BaseSparkTransform {
  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val srcField = config.getString(SOURCE_FILED)
    val key = config.getString(FIELDS)

    val func: UserDefinedFunction = udf((s: String) => {
      replace(
        s,
        config.getString(PATTERN),
        config.getString(REPLACEMENT),
        config.getBoolean(REPLACE_REGEX),
        config.getBoolean(REPLACE_FIRST))
    })
    var filterDf = df.withColumn(Constants.ROW_TMP, func(col(srcField)))
    filterDf = filterDf.withColumn(key, col(Constants.ROW_TMP))
    val ds = filterDf.drop(Constants.ROW_TMP)
    if (func != null) {
      env.getSparkSession.udf.register(UDF_NAME, func)
    }
    ds
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, FIELDS, PATTERN, REPLACEMENT)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        SOURCE_FILED -> DEFAULT_SOURCE_FILED,
        REPLACE_REGEX -> DEFAULT_REPLACE_REGEX,
        REPLACE_FIRST -> DEFAULT_REPLACE_FIRST))
    config = config.withFallback(defaultConfig)
  }

  @VisibleForTesting
  def replace(
      str: String,
      pattern: String,
      replacement: String,
      isRegex: Boolean,
      replaceFirst: Boolean): String = {

    if (isRegex) {
      if (replaceFirst) pattern.replaceFirstIn(str, replacement)
      else pattern.replaceAllIn(str, replacement)
    } else {
      val max = if (replaceFirst) 1 else -1
      StringUtils.replace(str, pattern, replacement, max)
    }
  }

  implicit def toReg(pattern: String): Regex = pattern.r

  override def getPluginName: String = PLUGIN_NAME
}

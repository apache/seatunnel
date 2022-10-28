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

import org.apache.seatunnel.common.Constants
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.seatunnel.spark.transform.SplitConfig._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

class Split extends BaseSparkTransform {

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val srcField = config.getString(SOURCE_FILED)
    val keys = config.getStringList(FIELDS)

    // https://stackoverflow.com/a/33345698/1145750
    var func: UserDefinedFunction = null
    val ds = config.getString(TARGET_FILED) match {
      case Constants.ROW_ROOT =>
        func = udf((s: String) => {
          split(s, config.getString(SPLIT_SEPARATOR), keys.size())
        })
        var filterDf = df.withColumn(Constants.ROW_TMP, func(col(srcField)))
        for (i <- 0 until keys.size()) {
          filterDf = filterDf.withColumn(keys.get(i), col(Constants.ROW_TMP)(i))
        }
        filterDf.drop(Constants.ROW_TMP)
      case targetField: String =>
        func = udf((s: String) => {
          val values = split(s, config.getString(SPLIT_SEPARATOR), keys.size)
          val kvs = (keys zip values).toMap
          kvs
        })
        df.withColumn(targetField, func(col(srcField)))
    }
    if (func != null) {
      env.getSparkSession.udf.register(UDF_NAME, func)
    }
    ds
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, FIELDS)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        SPLIT_SEPARATOR -> DEFAULT_SPLIT_SEPARATOR,
        SOURCE_FILED -> DEFAULT_SOURCE_FILED,
        TARGET_FILED -> Constants.ROW_ROOT))
    config = config.withFallback(defaultConfig)
  }

  /**
   * Split string by separator, if size of splited parts is less than fillLength,
   * empty string is filled; if greater than fillLength, parts will be truncated.
   */
  private def split(str: String, separator: String, fillLength: Int): Seq[String] = {
    val parts = str.split(separator).map(_.trim)
    val filled = fillLength compare parts.length match {
      case 0 => parts
      case 1 => parts ++ Array.fill[String](fillLength - parts.length)("")
      case -1 => parts.slice(0, fillLength)
    }
    filled.toSeq
  }

  override def getPluginName: String = PLUGIN_NAME
}

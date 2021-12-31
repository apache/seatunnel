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

import org.apache.seatunnel.common.RowConstant
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Split extends BaseSparkTransform {

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val srcField = config.getString("source_field")
    val keys = config.getStringList("fields")

    // https://stackoverflow.com/a/33345698/1145750
    config.getString("target_field") match {
      case RowConstant.ROOT => {
        val func = udf((s: String) => {
          split(s, config.getString("delimiter"), keys.size())
        })
        var filterDf = df.withColumn(RowConstant.TMP, func(col(srcField)))
        for (i <- 0 until keys.size()) {
          filterDf = filterDf.withColumn(keys.get(i), col(RowConstant.TMP)(i))
        }
        filterDf.drop(RowConstant.TMP)
      }
      case targetField: String => {
        val func = udf((s: String) => {
          val values = split(s, config.getString("delimiter"), keys.size)
          val kvs = (keys zip values).toMap
          kvs
        })

        df.withColumn(targetField, func(col(srcField)))
      }
    }
  }

  override def checkConfig(): CheckResult = {
    config.hasPath("fields") && config.getStringList("fields").size() > 0 match {
      case true => new CheckResult(true, "")
      case false => new CheckResult(false, "please specify [fields] as a non-empty string list")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "delimiter" -> " ",
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT))
    config = config.withFallback(defaultConfig)
  }

  /**
   * Split string by delimiter, if size of splited parts is less than fillLength,
   * empty string is filled; if greater than fillLength, parts will be truncated.
   */
  private def split(str: String, delimiter: String, fillLength: Int): Seq[String] = {
    val parts = str.split(delimiter).map(_.trim)
    val filled = fillLength compare parts.size match {
      case 0 => parts
      case 1 => parts ++ Array.fill[String](fillLength - parts.size)("")
      case -1 => parts.slice(0, fillLength)
    }
    filled.toSeq
  }
}

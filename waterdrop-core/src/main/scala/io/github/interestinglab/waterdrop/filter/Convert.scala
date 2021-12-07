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
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

class Convert extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    if (!conf.hasPath("source_field")) {
      (false, "please specify [source_field] as a non-empty string")
    } else if (!conf.hasPath("new_type")) {
      (false, "please specify [new_type] as a non-empty string")
    } else {
      (true, "")
    }
  }

  override def prepare(spark: SparkSession): Unit = {

    super.prepare(spark)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val srcField = conf.getString("source_field")
    val newType = conf.getString("new_type")

    newType match {
      case "string" => df.withColumn(srcField, col(srcField).cast(StringType))
      case "integer" => df.withColumn(srcField, col(srcField).cast(IntegerType))
      case "double" => df.withColumn(srcField, col(srcField).cast(DoubleType))
      case "float" => df.withColumn(srcField, col(srcField).cast(FloatType))
      case "long" => df.withColumn(srcField, col(srcField).cast(LongType))
      case "boolean" => df.withColumn(srcField, col(srcField).cast(BooleanType))
      case _: String => df
    }
  }
}

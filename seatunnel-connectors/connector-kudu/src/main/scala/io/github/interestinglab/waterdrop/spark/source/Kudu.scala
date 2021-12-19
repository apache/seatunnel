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

package io.github.interestinglab.waterdrop.spark.source

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource

class Kudu extends SparkBatchSource {

  override def checkConfig(): CheckResult = {
    config.hasPath("kudu_master") && config.hasPath("kudu_table") match {
      case true => new CheckResult(true, "")
      case false => new CheckResult(false, "please specify [kudu_master] and [kudu_table]")
    }
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val mapConf = Map(
      "kudu.master" -> config.getString("kudu_master"),
      "kudu.table" -> config.getString("kudu_table"))

    val ds = env.getSparkSession.read
      .format("org.apache.kudu.spark.kudu")
      .options(mapConf)
      .kudu
    ds
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {}
}

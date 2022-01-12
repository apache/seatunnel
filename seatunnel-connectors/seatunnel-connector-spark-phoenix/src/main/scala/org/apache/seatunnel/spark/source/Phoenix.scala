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

import scala.collection.JavaConverters._

import org.apache.phoenix.spark.ZkConnectUtil._
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

class Phoenix extends SparkBatchSource with Logging {

  var phoenixCfg: Map[String, String] = _
  val phoenixPrefix = "phoenix"

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    import org.apache.phoenix.spark.sparkExtend._
    env.getSparkSession.sqlContext.phoenixTableAsDataFrame(
      table = phoenixCfg(s"$phoenixPrefix.table"),
      columns = {
        if (config.hasPath("columns")) config.getStringList("columns").asScala else Seq()
      },
      zkUrl = Some(phoenixCfg(s"$phoenixPrefix.zk-connect")),
      predicate =
        if (phoenixCfg.contains(s"$phoenixPrefix.predicate")) {
          Some(phoenixCfg(s"$phoenixPrefix.predicate"))
        } else {
          None
        },
      tenantId =
        if (phoenixCfg.contains(s"$phoenixPrefix.tenantId")) {
          Some(phoenixCfg(s"$phoenixPrefix.tenantId"))
        } else {
          None
        })
  }

  override def checkConfig(): CheckResult = {
    val checkResult = CheckConfigUtil.check(config, "zk-connect", "table")
    if (checkResult.isSuccess) {
      checkZkConnect(config.getString("zk-connect"))
    }
    checkResult
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    phoenixCfg = config.entrySet().asScala.map {
      entry => s"$phoenixPrefix.${entry.getKey}" -> String.valueOf(entry.getValue.unwrapped())
    }.toMap

    printParams()
  }

  def printParams(): Unit = {
    phoenixCfg.foreach {
      case (key, value) => logInfo("[INFO] \t" + key + " = " + value)
    }
  }

}

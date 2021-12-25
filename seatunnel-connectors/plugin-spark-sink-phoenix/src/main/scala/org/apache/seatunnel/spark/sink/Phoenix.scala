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
package org.apache.seatunnel.spark.sink

import org.apache.commons.lang3.StringUtils
import org.apache.phoenix.spark.ZkConnectUtil._
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.util.Try

class Phoenix extends SparkBatchSink with Logging {

  var phoenixCfg: Map[String, String] = _
  val phoenixPrefix = "phoenix"

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    import org.apache.phoenix.spark.sparkExtend._
    data.saveToPhoenix(
      zkUrl = Some(phoenixCfg(s"$phoenixPrefix.zk-connect")),
      tableName = phoenixCfg(s"$phoenixPrefix.table"),
      tenantId = {
        if (phoenixCfg.contains(s"$phoenixPrefix.tenantId")) {
          Some(phoenixCfg(s"$phoenixPrefix.tenantId"))
        } else {
          None
        }
      },
      skipNormalizingIdentifier = {
        Try {
          if (config.hasPath("skipNormalizingIdentifier")) {
            config.getBoolean("skipNormalizingIdentifier")
          } else {
            false
          }
        }.getOrElse(false)
      })
  }

  override def checkConfig(): CheckResult = {
    if (config.hasPath("zk-connect") && config.hasPath("table") && StringUtils.isNotBlank(
      config.getString("zk-connect"))) {
      checkZkConnect(config.getString("zk-connect"))
      new CheckResult(true, "")
    } else {
      new CheckResult(false, "please specify [zk-connect] as a non-empty string")
    }
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

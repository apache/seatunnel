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
package org.apache.seatunnel.spark.jdbc.sink

import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConversions._

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.jdbc.Config
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode

class Jdbc extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val saveMode = config.getString("saveMode")
    var user: String = null

    try {
      user = config.getString(Config.USERNAME)
    } catch {
      case _: RuntimeException =>
        user = config.getString(Config.USE)
    }

    if ("update".equals(saveMode)) {
      data.write.format("org.apache.spark.sql.execution.datasources.jdbc2").options(
        Map(
          "saveMode" -> JDBCSaveMode.Update.toString,
          "driver" -> config.getString(Config.DRIVER),
          "url" -> config.getString(Config.URL),
          "user" -> user,
          "password" -> config.getString(Config.PASSWORD),
          "dbtable" -> config.getString(Config.DB_TABLE),
          "useSsl" -> config.getString(Config.USE_SSL),
          "isolationLevel" -> config.getString(Config.ISOLATION_LEVEL),
          "customUpdateStmt" -> config.getString(
            Config.CUSTOM_UPDATE_STMT
          ), // Custom mysql duplicate key update statement when saveMode is update
          "duplicateIncs" -> config.getString(Config.DUPLICATE_INCS),
          "showSql" -> config.getString(Config.SHOW_SQL))).save()
    } else {
      val prop = new java.util.Properties()
      prop.setProperty("driver", config.getString(Config.DRIVER))
      prop.setProperty("user", user)
      prop.setProperty("password", config.getString(Config.PASSWORD))
      data.write.mode(saveMode).jdbc(config.getString(Config.URL), config.getString(Config.DB_TABLE), prop)
    }

  }

  override def checkConfig(): CheckResult = {
    var checkResult =
      checkAllExists(config, Config.DRIVER, Config.URL, Config.DB_TABLE, Config.USERNAME, Config.PASSWORD)
    if (!checkResult.isSuccess) {
      val checkResultOld = checkAllExists(config, Config.DRIVER, Config.URL, Config.DB_TABLE, Config.USE, Config.PASSWORD)
      if(checkResultOld.isSuccess) {
        checkResult = checkResultOld
      }
    }
    checkResult
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "saveMode" -> "error",
        "useSsl" -> "false",
        "showSql" -> "true",
        "isolationLevel" -> "READ_UNCOMMITTED",
        "customUpdateStmt" -> "",
        "duplicateIncs" -> ""))
    config = config.withFallback(defaultConfig)
  }

  override def getPluginName: String = "Jdbc"
}

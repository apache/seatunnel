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

import org.apache.commons.collections.CollectionUtils
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.{JdbcConfigs, SparkEnvironment}
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, Statement}
import scala.collection.JavaConversions._

class Jdbc extends SparkBatchSink {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[Jdbc])

  private var jdbcConfigs: JdbcConfigs = _

  private def getConnection(): Connection = {
    Class.forName(jdbcConfigs.driver)
    val connection = DriverManager.getConnection(jdbcConfigs.url, jdbcConfigs.user, jdbcConfigs.password)
    connection
  }

  private def executeSql(sqls: List[String]): Unit = {
    val connection: Connection = getConnection()
    val statement: Statement = connection.createStatement()
    try {
      sqls.foreach { sql =>
        LOG.info(s"Starting to execute sql: $sql.")
        executeStatement(statement, sql)
        LOG.info(s"Executed sql: $sql successfully.")
      }
    } finally {
      if (statement != null) {
        statement.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }

  private def executePreSql(): Unit = {
    val preSqls = jdbcConfigs.preSqls
    if (CollectionUtils.isNotEmpty(preSqls)) {
      LOG.info(s"Starting to execute pre sqls: ${preSqls.mkString(";")}")
      executeSql(preSqls)
    }
  }

  private def executePostSql(): Unit = {
    val postSqls = jdbcConfigs.postSqls
    if (CollectionUtils.isNotEmpty(postSqls)) {
      LOG.info(s"Starting to execute post sqls: ${postSqls.mkString(";")}")
      executeSql(postSqls)
    }
  }

  private def executeStatement(statement: Statement, sql: String): Unit = {
    statement.executeUpdate(sql)
  }

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    executePreSql()
    val saveMode = jdbcConfigs.saveMode
    if ("update".equals(saveMode)) {
      data.write.format("org.apache.spark.sql.execution.datasources.jdbc2").options(
        Map(
          "saveMode" -> JDBCSaveMode.Update.toString,
          "driver" -> jdbcConfigs.driver,
          "url" -> jdbcConfigs.url,
          "user" -> jdbcConfigs.user,
          "password" -> jdbcConfigs.password,
          "dbtable" -> jdbcConfigs.dbTable,
          "useSsl" -> jdbcConfigs.useSsl,
          "customUpdateStmt" -> jdbcConfigs.customUpdateStmt,
          "duplicateIncs" -> jdbcConfigs.duplicateIncs,
          "showSql" -> jdbcConfigs.showSql)).save()
    } else {
      val prop = new java.util.Properties()
      prop.setProperty("driver", jdbcConfigs.driver)
      prop.setProperty("user", jdbcConfigs.user)
      prop.setProperty("password", jdbcConfigs.password)
      data.write.mode(saveMode).jdbc(jdbcConfigs.url, jdbcConfigs.dbTable, prop)
    }
    executePostSql()
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, JdbcConfigs.DRIVER, JdbcConfigs.URL, JdbcConfigs.DB_TABLE, JdbcConfigs.USER, JdbcConfigs.PASSWORD)
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "saveMode" -> "error",
        "useSsl" -> "false",
        "showSql" -> "true",
        "customUpdateStmt" -> "",
        "duplicateIncs" -> ""))
    config = config.withFallback(defaultConfig)
    jdbcConfigs = JdbcConfigs(config)
  }
}

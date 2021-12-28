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
package io.github.interestinglab.waterdrop.action

import java.sql.{Connection, DriverManager, SQLException}

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.github.interestinglab.waterdrop.apis.BaseAction

import scala.collection.JavaConverters._


/**
 * Jdbc Action is able to specify driver class while Mysql Output's driver is bound to com.mysql.jdbc.Driver etc.
 * When using Jdbc Action, class of jdbc driver must can be found in classpath.
 * Jdbc Action supports at least: MySQL, Oracle, PostgreSQL, SQLite
 * the sql executed can not return anything
 * */
class Jdbc extends BaseAction {

  var firstProcess = true
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

    val requiredOptions = List("driver", "url", "user", "password")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }
    if (nonExistsOptions.nonEmpty) {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    }
    (true, "")
  }

  override def onExecutionStarted(sparkSession: SparkSession, sparkConf: SparkConf, config: Config): Unit = {
    executeSql("before")
  }

  override def onExecutionFinished(sparkConf: SparkConf, config: Config): Unit = {
    executeSql("after")
  }

  def getConnection(driver: String, url: String, user: String, password: String) : Connection = {
    Class.forName(driver)
    DriverManager.getConnection(url, user, password)
  }

  def executeSql(executeType: String): Unit = {
    if (config.hasPath(executeType)) {
      val actionConfig = config.getConfig(executeType)
      if (actionConfig.hasPath("actions")) {
        val configList = actionConfig.getList("actions")
        val ignoreErrorSql = if (actionConfig.hasPath("errorSqlIgnored")) actionConfig.getBoolean("errorSqlIgnored") else false
        if (configList != null && configList.size() > 0) {
          val dataBaseConfig = getDataBaseConfig();
          val connection = getConnection(dataBaseConfig._1, dataBaseConfig._2, dataBaseConfig._3, dataBaseConfig._4)
          val statement = connection.createStatement()
          configList.asScala.foreach {
            sql =>
              try {
                statement.execute(sql.unwrapped().toString)
              } catch {
                case e: SQLException => {
                  if (!ignoreErrorSql) {
                    throw new SQLException(e)
                  }
                }
              }
          }
          statement.close()
          connection.close()
        }
      }
    }
  }

  def getDataBaseConfig(): (String, String, String, String) = {
    val driver = config.getString("driver")
    val url = config.getString("url")
    val user = config.getString("user")
    val password = config.getString("password")
    (driver, url, user, password)
  }
}

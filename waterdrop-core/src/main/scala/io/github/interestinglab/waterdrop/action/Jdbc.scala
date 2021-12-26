package io.github.interestinglab.waterdrop.action

import java.sql.{Connection, DriverManager}

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
    executeSql("beforeActions")
  }

  override def onExecutionFinished(sparkConf: SparkConf, config: Config): Unit = {
    executeSql("afterActions")
  }

  def getConnection(driver: String, url: String, user: String, password: String) : Connection = {
    Class.forName(driver)
    DriverManager.getConnection(url, user, password)
  }

  def executeSql(executeType: String): Unit = {
    val configList = config.getList(executeType)
    if (configList != null && configList.size() > 0) {
      val dataBaseConfig = getDataBaseConfig();
      val connection = getConnection(dataBaseConfig._1, dataBaseConfig._2, dataBaseConfig._3, dataBaseConfig._4)
      val statement = connection.createStatement()
      configList.asScala.foreach(sql => statement.execute(sql.unwrapped().toString))
      statement.close()
      connection.close()
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

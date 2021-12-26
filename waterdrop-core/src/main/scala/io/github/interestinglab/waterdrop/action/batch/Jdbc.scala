package io.github.interestinglab.waterdrop.action.batch

import io.github.interestinglab.waterdrop.apis.BaseAction
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Jdbc Action is able to specify driver class while Mysql Output's driver is bound to com.mysql.jdbc.Driver etc.
 * When using Jdbc Action, class of jdbc driver must can be found in classpath.
 * Jdbc Action supports at least: MySQL, Oracle, PostgreSQL, SQLite

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
    /*"beforeActions", "afterActions"*/
  }

  override def onExecutionFinished(sparkConf: SparkConf, config: Config): Unit = {
    /*"beforeActions", "afterActions"*/
  }

}

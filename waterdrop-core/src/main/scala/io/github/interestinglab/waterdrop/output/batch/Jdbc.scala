package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.JavaConversions._

/**
 * Jdbc Output is able to specify driver class while Mysql Output's driver is bound to com.mysql.jdbc.Driver etc.
 * When using Jdbc Output, class of jdbc driver must can be found in classpath.
 * JDBC Output supports at least: MySQL, Oracle, PostgreSQL, SQLite

 * */
class Jdbc extends BaseOutput {

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

    // TODO: are user, password required ?
    val requiredOptions = List("driver", "url", "table", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length == 0) {

      val saveModeAllowedValues = List("overwrite", "append", "ignore", "error");

      if (!config.hasPath("save_mode") || saveModeAllowedValues.contains(config.getString("save_mode"))) {
        (true, "")
      } else {
        (false, "wrong value of [save_mode], allowed values: " + saveModeAllowedValues.mkString(", "))
      }

    } else {
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
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append" // allowed values: overwrite, append, ignore, error
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {

    val prop = new java.util.Properties
    prop.setProperty("driver", config.getString("driver"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    val saveMode = config.getString("save_mode")

    if (firstProcess) {
      df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)
      firstProcess = false
    } else if (saveMode == "overwrite") {
      // actually user only want the first time overwrite in streaming(generating multiple dataframe)
      df.write.mode(SaveMode.Append).jdbc(config.getString("url"), config.getString("table"), prop)
    } else {
      df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)
    }
  }
}

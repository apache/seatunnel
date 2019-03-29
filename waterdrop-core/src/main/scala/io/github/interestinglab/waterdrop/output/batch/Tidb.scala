package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.JavaConversions._

class Tidb extends BaseOutput {

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

    val requiredOptions = List("url", "table", "user", "password");

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
          .map { case (option) => "[" + option + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append", // allowed values: overwrite, append, ignore, error
        "useSSL" -> "false",
        "isolationLevel" -> "NONE",
        "batchsize" -> 150
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("useSSL", config.getString("useSSL"))
    prop.setProperty("isolationLevel", config.getString("isolationLevel"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))
    prop.setProperty(JDBCOptions.JDBC_BATCH_INSERT_SIZE, config.getString("batchsize"))

    val saveMode = config.getString("save_mode")

    firstProcess match {
      case true =>
        df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)
        firstProcess = false
      case false =>
        saveMode match {
          case "overwrite" =>
            // actually user only want the first time overwrite in streaming(generating multiple dataframe)
            df.write.mode(SaveMode.Append).jdbc(config.getString("url"), config.getString("table"), prop)
          case _ =>
            df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop)

        }
    }
  }
}

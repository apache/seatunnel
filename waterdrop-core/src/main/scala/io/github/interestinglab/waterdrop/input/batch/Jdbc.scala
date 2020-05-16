package io.github.interestinglab.waterdrop.input.batch

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils

import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._


class Jdbc extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "table", "user", "password", "driver");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  def jdbcReader(sparkSession: SparkSession, driver: String): DataFrameReader = {

    val reader = sparkSession.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", driver)

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false)) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader.options(optionMap)
      }
      case Failure(exception) => // do nothing
    }

    reader
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, config.getString("driver")).load()
  }
}

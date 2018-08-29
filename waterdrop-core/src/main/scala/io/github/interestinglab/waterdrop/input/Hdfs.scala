package io.github.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * HDFS Static Input to read hdfs files in csv, json, parquet, parquet format.
 * */
class Hdfs extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "format" -> "json"
      )
    )

    this.config = config.withFallback(defaultConfig)
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    this.config.hasPath("path") match {
      case true => (true, "")
      case false => (false, "please specify [path] as string")
    }
  }

  /**
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {

    var reader = spark.read.format(config.getString("format"))

    Try(config.getConfig("options")) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader = reader.options(optionMap)
      }
      case Failure(exception) => // do nothing

    }

    reader.load(config.getString("path"))
  }
}

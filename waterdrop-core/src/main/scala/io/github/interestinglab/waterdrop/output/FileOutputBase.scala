package io.github.interestinglab.waterdrop.output

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.utils.StringTemplate
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

abstract class FileOutputBase extends BaseOutput {

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

  protected def checkConfigImpl(allowedURISchema: List[String]): (Boolean, String) = {

    config.hasPath("path") && !config.getString("path").trim.isEmpty match {
      case true => {
        val dir = config.getString("path")
        val unSupportedSchema = allowedURISchema.forall(schema => {
          // there are 3 "/" in dir, first 2 are from URI Schema, and the last is from path
          !dir.startsWith(schema + "/")
        })

        unSupportedSchema match {
          case true =>
            (false, "unsupported schema, please set the following allowed schemas: " + allowedURISchema.mkString(", "))
          case false => (true, "")
        }
      }
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "partition_by" -> util.Arrays.asList(),
        "save_mode" -> "error", // allowed values: overwrite, append, ignore, error
        "serializer" -> "json", // allowed values: csv, json, parquet, text
        "path_time_format" -> "yyyyMMddHHmmss" // if variable 'now' is used in path, this option specifies its time_format
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: DataFrame): Unit = {

    var writer = df.write.mode(config.getString("save_mode"))

    writer = config.getStringList("partition_by").length == 0 match {
      case true => writer
      case false => {
        val partitionKeys = config.getStringList("partition_by")
        writer.partitionBy(partitionKeys: _*)
      }
    }

    Try(config.getConfig("option")) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        writer.options(optionMap)
      }
      case Failure(exception) => // do nothing

    }

    val path = StringTemplate.substitute(config.getString("path"), config.getString("path_time_format"))
    config.getString("serializer") match {
      case "csv" => writer.csv(path)
      case "json" => writer.json(path)
      case "parquet" => writer.parquet(path)
      case "text" => writer.text(path)
    }
  }
}

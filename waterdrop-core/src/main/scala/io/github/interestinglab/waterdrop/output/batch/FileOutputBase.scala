package io.github.interestinglab.waterdrop.output.batch

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.utils.StringTemplate
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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

        dir.startsWith("/") || uriInAllowedSchema(dir, allowedURISchema) match {
          case true => (true, "")
          case false =>
            (false, "invalid path URI, please set the following allowed schemas: " + allowedURISchema.mkString(", "))
        }
      }
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  /**
   * check if the schema name in this uri is allowed.
   * @return true if schema name is allowed
   * */
  protected def uriInAllowedSchema(uri: String, allowedURISchema: List[String]): Boolean = {

    val notAllowed = allowedURISchema.forall(schema => {
      !uri.startsWith(schema)
    })

    !notAllowed
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }

    path
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

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

  def processImpl(df: Dataset[Row], defaultUriSchema: String): Unit = {

    var writer = df.write.mode(config.getString("save_mode"))

    writer = config.getStringList("partition_by").length == 0 match {
      case true => writer
      case false => {
        val partitionKeys = config.getStringList("partition_by")
        writer.partitionBy(partitionKeys: _*)
      }
    }

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false)) match {

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

    var path = buildPathWithDefaultSchema(config.getString("path"), defaultUriSchema)
    path = StringTemplate.substitute(path, config.getString("path_time_format"))
    config.getString("serializer") match {
      case "csv" => writer.csv(path)
      case "json" => writer.json(path)
      case "parquet" => writer.parquet(path)
      case "text" => writer.text(path)
      case "orc" => writer.orc(path)
    }
  }
}

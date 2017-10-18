package org.interestinglab.waterdrop.output

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class Textfile(var config: Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("path") && !config.getString("path").trim.isEmpty match {
      case true => {
        val allowedURISchema = List("file://", "hdfs://", "s3://", "s3a://", "s3n://")
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
        "partition_by" -> List(),
        "save_mode" -> "error", // allowed values: overwrite, append, ignore, error
        "serializer" -> "json" // allowed values: csv, json, parquet, text
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: DataFrame): Unit = {

    // TODO: 改变mode不好使
    df.write.mode(config.getString("save_mode"))

    val writer = config.getStringList("partition_by").length == 0 match {
      case true => df.write
      case false => {
        val partitionKeys = config.getStringList("partition_by")
        df.write.partitionBy(partitionKeys: _*)
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

    // TODO: 涉及到path通配的问题,可以指定生成规则uuid, time_uuid, time
    val path = config.getString("path") + "/" + UUID.randomUUID().toString
    config.getString("serializer") match {
      case "csv" => writer.csv(path)
      case "json" => writer.json(path)
      case "parquet" => writer.parquet(path)
      case "text" => writer.text(path)
    }
  }
}

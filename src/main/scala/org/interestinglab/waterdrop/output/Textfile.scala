package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class Textfile(var config: Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = {
    // TODO: uri schema
    config.hasPath("path") && !config.getString("path").trim.isEmpty match {
      case true => (true, "")
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "error", // allowed values: overwrite, append, ignore, error
        "serializer" -> "json" // allowed values: csv, json, orc, parquet, text
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: DataFrame): Unit = {

    df.write.mode(config.getString("save_mode"))

    val path = config.getString("path")
    config.getString("serializer") match {
      case "csv" => df.write.csv(path)
      case "json" => df.write.json(path) // TODO: 如果是file://, 文件会被保存到各个executor所在的server本地？
      case "orc" => df.write.orc(path) // TODO: 需要指定依赖吗？
      case "parquet" => df.write.parquet(path) // TODO: 需要指定依赖吗？
      case "text" => df.write.text(path)
    }
    // TODO: specify partitionBy, options
  }
}

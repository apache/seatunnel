package io.github.interestinglab.waterdrop.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutputIntra
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Stdout extends BaseStructuredStreamingOutputIntra {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    df.writeStream
      .format("console")
      .outputMode(config.getString("outputMode"))
  }
}

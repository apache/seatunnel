package io.github.interestinglab.waterdrop.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutputIntra
import io.github.interestinglab.waterdrop.output.utils.StructuredUtils
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Stdout extends BaseStructuredStreamingOutputIntra {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {
    StructuredUtils.checkTriggerMode(config) match {
      case true => (true, "")
      case false => (false, "please specify [interval] when [triggerMode] is ProcessingTime or Continuous")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "streaming_output_mode" -> "Append",
        "trigger_type" -> "default"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {

    var writer = df.writeStream
      .format("console")
      .outputMode(config.getString("streaming_output_mode"))

    writer = StructuredUtils.setCheckpointLocation(writer, config)

    StructuredUtils.writeWithTrigger(config, writer)
  }
}

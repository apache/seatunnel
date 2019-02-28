package io.github.interestinglab.waterdrop.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutputIntra
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Stdout extends BaseStructuredStreamingOutputIntra {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {
    if (config.hasPath("triggerMode")) {
      val triggerMode = config.getString("triggerMode")
      triggerMode match {
        case "ProcessingTime" | "Continuous" => {
          if (config.hasPath("interval")) {
            (true, "")
          } else {
            (false, "please specify [interval] when [triggerMode] is ProcessingTime or Continuous")
          }
        }
        case _ => (true, "")
      }
    } else {
      (true, "")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append",
        "triggerMode" -> "default"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {

    val triggerMode = config.getString("triggerMode")
    val writer = df.writeStream
      .format("console")
      .outputMode(config.getString("outputMode"))

    triggerMode match {
      case "default" => writer
      case "ProcessingTime" => writer.trigger(Trigger.ProcessingTime(config.getString("interval")))
      case "OneTime" => writer.trigger(Trigger.Once())
      case "Continuous" => writer.trigger(Trigger.Continuous(config.getString("interval")))
    }
  }
}

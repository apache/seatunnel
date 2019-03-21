package io.github.interestinglab.waterdrop.output.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutputIntra
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Kafka extends BaseStructuredStreamingOutputIntra {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {

    !config.hasPath("producer.bootstrap.servers") || !config.hasPath("topic") match {
      case true => (false, "please specify [producer.bootstrap.servers] and [topic]")
      case false => {
        StructuredUtils.checkTriggerMode(config) match {
          case true => (true, "")
          case false => (false, "please specify [interval] when [triggerMode] is ProcessingTime or Continuous")
        }
      }
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

    var writer = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("producer.bootstrap.servers"))
      .option("topic", config.getString("topic"))
      .outputMode(config.getString("outputMode"))
    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config,writer)
  }

}

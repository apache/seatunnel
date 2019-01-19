package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row}


class Console extends BaseStructuredStreamingOutput{

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config=config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = (true,"")

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    df.writeStream.format("console")
      .outputMode(config.getString("outputMode"))
  }
}

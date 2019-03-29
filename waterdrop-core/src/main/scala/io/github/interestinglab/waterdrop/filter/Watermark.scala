package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Watermark extends BaseFilter {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("event_time") && config.hasPath("delay_threshold") match {
      case true => (true, "")
      case false => (false, "please specify [event_time] and [delay_threshold] ")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    df.withWatermark(config.getString("event_time"), config.getString("delay_threshold"))
  }
}

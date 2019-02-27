package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.JavaConversions._

class WaterMaker extends BaseFilter {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("timestamp.field") && config.hasPath("timestamp.deadline") match {
      case true => (true, "")
      case false => (false, "please specify [timestamp.field && timestamp.deadline] ")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "table_name" -> "water_maker_tb"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val watermark = df.withWatermark(config.getString("timestamp.field"), config.getString("timestamp.deadline"))
    watermark.createOrReplaceTempView(config.getString("table_name"))
    watermark
  }
}

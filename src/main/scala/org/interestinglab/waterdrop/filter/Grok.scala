package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Grok(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  // TODO
  override def checkConfig(): (Boolean, String) = (true, "")

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    // TODO
    df
  }
}

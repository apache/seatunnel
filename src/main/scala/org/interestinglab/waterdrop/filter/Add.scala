package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

class Add(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = super.prepare(spark, ssc)

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    df
  }
}

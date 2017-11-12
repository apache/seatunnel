package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class Truncate(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> "truncated",
        "max_length" -> 256
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val truncateUDF = udf((str: String) => truncate(str))
    df.withColumn(conf.getString("target_field"), truncateUDF(col(conf.getString("source_field"))))
  }

  private def truncate(str: String): String = {
    if (str.length <= conf.getInt("max_length")) {
      str
    } else {
      str.take(conf.getInt("max_length"))
    }
  }
}

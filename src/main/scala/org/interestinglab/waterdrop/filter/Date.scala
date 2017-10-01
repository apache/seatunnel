package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

class Date(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> "datetime",
        "from_time_format" -> "", // TODO:
        "to_time_format" -> "", // TODO:
        "time_zone" -> "" // TODO:
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    // TODO: 新增一个Date类型的Field ? 或者从一个字符串转换时间格式到另一个字符串？
    df
  }
}

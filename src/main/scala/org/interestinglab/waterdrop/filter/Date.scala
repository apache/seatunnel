package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.interestinglab.waterdrop.utils.{FormatParser, StringTemplate}

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
        "source_field" -> Json.ROOT,
        "target_field" -> "datetime",
        "source_time_format" -> "dd/MMM/yyyy:HH:mm:ss Z", // TODO:
        "target_time_format" -> "yyyy/mm/dd HH:mm:ss", // TODO:
        "time_zone" -> "", // TODO:
        "default_value" -> "${now}",
        "locale" -> "" // TODO：  语言环境
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val targetTimeFormat = conf.getString("target_time_format")
    val srcField = conf.getString("source_field")
    val targetField = conf.getString("target_field")
    val defaultValue = config.getString("default_value")
    // TODO: 新增一个Date类型的Field ? 或者从一个字符串转换时间格式到另一个字符串？
    conf.getString("source_time_format") match {

      case sourceTimeFormat: String => {

        val func = udf((s: String) => {
          val dateParser = new FormatParser(sourceTimeFormat, targetTimeFormat)
          val (success, dateTime) = dateParser.parse(s)
          if (success) {
            dateTime
          } else {
            StringTemplate.substitute(defaultValue, targetTimeFormat)
          }
        })

        df.withColumn(targetField, func(col(srcField)))
      }
    }
  }
}

package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.{col, regexp_replace}

class Replace extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    // replacement must present and ""(empty string) is its valid value
    conf.hasPath("pattern") && !conf.getString("pattern").trim.equals("") && conf.hasPath("replacement") match {
      case true => (true, "")
      case false => (false, "please specify [pattern] and [replacement] as string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> "replaced"
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    df.withColumn(
      conf.getString("target_field"),
      regexp_replace(col(conf.getString("source_field")), conf.getString("pattern"), conf.getString("replacement")))
  }
}

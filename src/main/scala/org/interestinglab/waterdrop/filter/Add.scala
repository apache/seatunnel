package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.{col, expr}
import scala.collection.JavaConversions._

class Add(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("target_field") && conf.hasPath("value") match {
      case true => (true, "")
      case false => (false, "please specify [target_field], [value]")
    }
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    df.withColumn(conf.getString("target_field"), expr(conf.getString("value")))
  }
}

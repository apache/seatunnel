package org.interestinglab.waterdrop.filter

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.{col, from_json}

class Json(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> Json.ROOT)
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    // TODO:
    df.withColumn("test", col(conf.getString("source_field")))
    //spark.sqlContext.read.json()
  }
}

object Json {
  val ROOT = "__root__"
}

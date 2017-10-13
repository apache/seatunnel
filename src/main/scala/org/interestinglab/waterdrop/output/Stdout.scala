package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

class Stdout(var config: Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "serializer" -> "plain"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: DataFrame): Unit = {

    if (config.hasPath("limit")) {
      df.show(config.getInt("limit"), false)
    } else {
      val num = 20
      df.show(num, false)
    }
  }
}

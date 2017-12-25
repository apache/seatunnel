package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class Stdout(var config: Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = {
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => (true, "")
      case false => (false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "limit" -> 100,
        "serializer" -> "plain" // plain | json
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: DataFrame): Unit = {

    val limit = config.getInt("limit")

    config.getString("serializer") match {
      case "plain" => {
        if (limit == -1) {
          df.show(Int.MaxValue, false)
        } else if (limit > 0) {
          df.show(limit, false)
        }
      }
      case "json" => {
        if (limit == -1) {
          df.toJSON.take(Int.MaxValue).foreach(s => println(s))

        } else if (limit > 0) {
          df.toJSON.take(limit).foreach(s => println(s))
        }
      }
    }
  }
}

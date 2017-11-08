package org.interestinglab.waterdrop.filter

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class Json(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("source_field") match {
      case true => (true, "")
      case false => (false, "please specify [source_field] as a non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map("source_field" -> "raw_message", "target_field" -> Json.ROOT)
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    val srcField = conf.getString("source_field")

    conf.getString("target_field") match {
      case Json.ROOT => df // TODO
      case targetField: String => {
        val func = udf((s: String) => {
          implicit val formats = DefaultFormats

          Try(JsonMethods.parse(s).extract[Map[String, String]]) match {
            case Success(result) => result
            case Failure(ex) => Map("_failure" -> ex.getMessage)
          }
        })

        df.withColumn(targetField, func(col(srcField)))
      }
    }
  }
}

object Json {
  val ROOT = "__root__"
}

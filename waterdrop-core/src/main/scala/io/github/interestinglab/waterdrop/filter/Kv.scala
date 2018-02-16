package io.github.interestinglab.waterdrop.filter

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.core.RowConstant
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConversions._

class Kv(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("include_fields") && conf.hasPath("exclude_fields") match {
      case true => (false, "[include_fields] and [exclude_fields] must not be specified at the same time")
      case false => (true, "")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "field_split" -> "&",
        "value_split" -> "=",
        "field_prefix" -> "",
        "include_fields" -> util.Arrays.asList(),
        "exclude_fields" -> util.Arrays.asList(),
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    conf.getString("target_field") match {
      case RowConstant.ROOT => df // TODO: implement
      case targetField: String => {
        val kvUDF = udf((s: String) => kv(s))
        df.withColumn(targetField, kvUDF(col(conf.getString("source_field"))))
      }
    }
  }

  private def kv(str: String): Map[String, String] = {

    val includeFields = conf.getStringList("include_fields")
    val excludeFields = conf.getStringList("exclude_fields")
    var map: Map[String, String] = Map()
    str
      .split(conf.getString("field_split"))
      .foreach(s => {
        val pair = s.split(conf.getString("value_split"))
        if (pair.size == 2) {
          val key = pair(0).trim
          val value = pair(1).trim

          if (includeFields.length == 0 && excludeFields.length == 0) {
            map += (conf.getString("field_prefix") + key -> value)
          } else if (includeFields.length > 0 && includeFields.contains(key)) {
            map += (conf.getString("field_prefix") + key -> value)
          } else if (excludeFields.length > 0 && !excludeFields.contains(key)) {
            map += (conf.getString("field_prefix") + key -> value)
          }
        }
      })
    map
  }
}

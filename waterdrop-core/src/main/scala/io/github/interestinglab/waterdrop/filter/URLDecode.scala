package io.github.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConversions._
import java.net.URLDecoder

import org.apache.spark.sql.expressions.UserDefinedFunction

class URLDecode extends BaseFilter {

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

  override def getUdfList(): List[(String, UserDefinedFunction)] = {
    val func = udf((source: String) => URLDecoder.decode(source, "utf-8"))
    List(("urldecode", func))
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message"
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val decodeFunc = getUdfList().get(0)._2
    val sourceField = conf.getString("source_field")
    var targetField = sourceField
    if (conf.hasPath("target_field")) {
      targetField = conf.getString("target_field")
    }
    df.withColumn(conf.getString("target_field"), decodeFunc(col(sourceField)))
  }
}

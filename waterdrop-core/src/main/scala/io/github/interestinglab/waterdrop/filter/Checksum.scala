package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.{col, crc32, md5, sha1}

import scala.collection.JavaConversions._

class Checksum(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    val allowedMethods = List("CRC32", "MD5", "SHA1")
    conf.hasPath("method") && !allowedMethods.contains(conf.getString("method").trim.toUpperCase) match {
      case true => (false, "method in [method] is not allowed, please specify one of " + allowedMethods.mkString(", "))
      case false => (true, "")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "method" -> "SHA1",
        "source_field" -> "raw_message",
        "target_field" -> "checksum"
      )
    )

    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val srcField = conf.getString("source_field")
    val column = conf.getString("method").toUpperCase match {
      case "SHA1" => sha1(col(srcField))
      case "MD5" => md5(col(srcField))
      case "CRC32" => crc32(col(srcField))
    }
    df.withColumn(conf.getString("target_field"), column)
  }
}

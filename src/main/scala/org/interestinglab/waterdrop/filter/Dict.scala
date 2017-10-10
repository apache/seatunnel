package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

class Dict(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  // parameters:
  // file_url, support protocol: hdfs, file, http
  // delimiter, column delimiter in row
  // headers
  // source_field
  // dict_field
  // join method: self implemented by broadcast variable, 2 dataframe join(make sure it is broadcast join)
  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("file_url") && conf.hasPath("headers") && conf.hasPath("source_field") && conf.hasPath("dict_field") match {
      case true => {
        val fileURL = conf.getString("file_url")
        val allowedProtocol = List("hdfs://", "file://", "http://")
        var unsupportedProtocol = true
        allowedProtocol.foreach(p => {
          if (fileURL.startsWith(p)) {
            unsupportedProtocol = false
          }
        })

        if (unsupportedProtocol) {
          (false, "unsupported protocol in [file_url], please choose one of " + allowedProtocol.mkString(", "))
        } else {
          (true, "")
        }
      }
      case false => (false, "please specify [file_url], [headers], [source_field], [dict_field]")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "delimiter" -> ","
      )
    )

    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    // TODO
    df
  }
}

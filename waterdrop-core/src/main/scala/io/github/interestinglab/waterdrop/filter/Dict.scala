package io.github.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Dict extends BaseFilter {

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

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "delimiter" -> ","
      )
    )

    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    // TODO
    df
  }
}

package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.elasticsearch.spark.sql._
import org.interestinglab.waterdrop.utils.StringTemplate

import scala.collection.JavaConversions._

class Elasticsearch(var config : Config) extends BaseOutput(config) {

  var esCfg: Map[String, String] = Map()

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("hosts") && config.getStringList("hosts").size() > 0 match {
      case true => {
        val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [hosts] as a non-empty string list")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "index" -> "waterdrop",
        "index_type" -> "log",
        "index_time_format" -> "yyyy.MM.dd",
        "batch_size_bytes" -> "1mb",
        "batch_size_entries" -> "10000"
      )
    )
    config = config.withFallback(defaultConfig)

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))
    esCfg += ("es.batch.size.bytes" -> config.getString("batch_size_bytes"))
    esCfg += ("es.batch.size.entries" -> config.getString("batch_size_entries"))
  }

  override def process(df: DataFrame): Unit = {
    val index = StringTemplate.substitute(config.getString("index"), config.getString("index_time_format"))
    df.saveToEs(index + "/" + config.getString("index_type"), this.esCfg)
  }
}

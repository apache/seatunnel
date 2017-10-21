package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.elasticsearch.spark.sql._

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
        "index_type" -> "log"
      )
    )
    config = config.withFallback(defaultConfig)

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))
  }

  override def process(df: DataFrame): Unit = {
    df.saveToEs(config.getString("index") + "/" + config.getString("index_type"), this.esCfg)
  }
}

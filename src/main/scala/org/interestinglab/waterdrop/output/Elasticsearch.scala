package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions._

class Elasticsearch(var config : Config) extends BaseOutput(config) {

  var esCfg: Map[String, String] = Map()

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("index") match {
      case true => (true, "")
      case false => (false, "please specify [index] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "index_type" -> "log"
      )
    )
    config = config.withFallback(defaultConfig)

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))
  }

  override def process(df: DataFrame): Unit = {
    df.saveToEs("spark/peple", this.esCfg)
  }
}

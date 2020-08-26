package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.config.ConfigFactory
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import io.github.interestinglab.waterdrop.common.utils.StringTemplate
import org.apache.spark.sql.{Dataset, Row}
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions._


class Elasticsearch extends SparkBatchSink {

  val esPrefix = "es."
  var esCfg: Map[String, String] = Map()

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val index = StringTemplate.substitute(config.getString("index"), config.getString("index_time_format"))
    df.saveToEs(index + "/" + config.getString("index_type"), this.esCfg)
  }

  override def checkConfig(): CheckResult = {
    config.hasPath("hosts") && config.getStringList("hosts").size() > 0 match {
      case true => {
        val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        new CheckResult(true, "")
      }
      case false => new CheckResult(false, "please specify [hosts] as a non-empty string list")
    }
  }

  override def prepare(environment: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "index" -> "waterdrop",
        "index_type" -> "log",
        "index_time_format" -> "yyyy.MM.dd"
      )
    )
    config = config.withFallback(defaultConfig)

    config
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey

        if (key.startsWith(esPrefix)) {
          val value = String.valueOf(entry.getValue.unwrapped())
          esCfg += (key -> value)
        }
      })

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))

    println("[INFO] Output ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

}

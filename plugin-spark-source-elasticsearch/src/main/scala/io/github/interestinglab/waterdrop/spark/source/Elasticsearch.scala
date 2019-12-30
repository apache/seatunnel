package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.{TypesafeConfigUtils, CheckResult}
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource

import org.apache.spark.sql.{Dataset, Row}
import scala.collection.JavaConversions._

class Elasticsearch extends SparkBatchSource {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."

  override def prepare(env: SparkEnvironment): Unit = {
    if (TypesafeConfigUtils.hasSubConfig(config, esPrefix)) {
      val esConfig = TypesafeConfigUtils.extractSubConfig(config, esPrefix, false)
      esConfig
        .entrySet()
        .foreach(entry => {
          val key = entry.getKey
          val value = String.valueOf(entry.getValue.unwrapped())
          esCfg += (esPrefix + key -> value)
        })
    }

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))

    println("[INFO] Input ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val index = config.getString("index")

    env.getSparkSession.read
      .format("org.elasticsearch.spark.sql")
      .options(esCfg)
      .load(index)

  }

  override def checkConfig(): CheckResult = {
    config.hasPath("hosts") && config.hasPath("index") && config.getStringList("hosts").size() > 0 match {
      case true => {
        // val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        new CheckResult(true, "")
      }
      case false => new CheckResult(false, "please specify [hosts] as a non-empty string list")
    }
  }

}

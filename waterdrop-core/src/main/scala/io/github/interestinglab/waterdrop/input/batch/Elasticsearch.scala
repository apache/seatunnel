package io.github.interestinglab.waterdrop.input.batch

import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Elasticsearch extends BaseStaticInput {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."
  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def prepare(spark: SparkSession): Unit = {

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

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("hosts") && config.hasPath("index") && config.getStringList("hosts").size() > 0 match {
      case true => {
        // val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [hosts] as a non-empty string list")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val index = config.getString("index")

    spark.read
      .format("org.elasticsearch.spark.sql")
      .options(esCfg)
      .load(index)
  }
}

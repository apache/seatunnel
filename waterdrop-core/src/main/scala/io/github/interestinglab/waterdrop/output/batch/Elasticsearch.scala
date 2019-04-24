package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.utils.StringTemplate
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions._

class Elasticsearch extends BaseOutput {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

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

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

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

  override def process(df: Dataset[Row]): Unit = {
    val index = StringTemplate.substitute(config.getString("index"), config.getString("index_time_format"))
    df.saveToEs(index + "/" + config.getString("index_type"), this.esCfg)
  }
}

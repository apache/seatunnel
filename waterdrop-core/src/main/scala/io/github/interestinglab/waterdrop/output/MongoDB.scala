package io.github.interestinglab.waterdrop.output

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class MongoDB extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    var defaultConfig = ConfigFactory.parseMap(
      Map(
        "isReplace" -> "false"
      )
    )
    this.config = config.withFallback(defaultConfig)
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("mongo_uri") && config.hasPath("database") && config.hasPath("collection")  match {
      case true => (true, "")
      case false => (false, "please specify [mongo_uri] and [database] and [collection] ")
    }
  }

  override def process(df: Dataset[Row]): Unit = {

    val writeConf = WriteConfig(Map(
      "uri" -> config.getString("mongo_uri"),
      "database" -> config.getString("database"),
      "collection" -> config.getString("collection"),
      "replaceDocument" -> config.getString("isReplace")
    ))
    MongoSpark.save(df,writeConf)
  }
}

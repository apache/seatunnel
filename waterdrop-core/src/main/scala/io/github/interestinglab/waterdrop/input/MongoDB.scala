package io.github.interestinglab.waterdrop.input

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class MongoDB extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    var defaultConfig = ConfigFactory.parseMap(
      Map(
        "partitioner" -> "MongoShardedPartitioner"
      )
    )
    this.config = config.withFallback(defaultConfig)
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("mongo_uri") && config.hasPath("database") && config.hasPath("collection") && config.hasPath("table_name") match {
      case true => (true, "you can please mongobd input partitioner,default is MongoShardedPartitioner")
      case false => (false, "please specify [mongo_uri] and [database] and [collection] and [table_name]")
    }
  }


  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val configur = ReadConfig(Map(
      "uri" -> config.getString("mongo_uri"),
      "spark.mongodb.input.partitioner" -> config.getString("partitioner"),
      "database" -> config.getString("database"),
      "collection" -> config.getString("collection")))
    MongoSpark.load(spark, configur)
  }

}

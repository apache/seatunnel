package io.github.interestinglab.waterdrop.input.batch

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class MongoDB extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  var readConfig: ReadConfig = _

  val confPrefix = "readconfig."

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    TypesafeConfigUtils.hasSubConfig(config, confPrefix) && config.hasPath("table_name") match {
      case true => {
        val read = TypesafeConfigUtils.extractSubConfig(config, confPrefix, false)
        read.hasPath("uri") && read.hasPath("database") && read.hasPath("collection") match {
          case true => (true, "")
          case false => (false, "please specify [readconfig.uri] and [readconfig.database] and [readconfig.collection]")
        }
      }
      case false => (false, "please specify [readconfig]  and [table_name]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val map = new collection.mutable.HashMap[String, String]

    TypesafeConfigUtils
      .extractSubConfig(config, confPrefix, false)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        map.put(key, value)
      })
    readConfig = ReadConfig(map)
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    MongoSpark.load(spark, readConfig)
  }

}

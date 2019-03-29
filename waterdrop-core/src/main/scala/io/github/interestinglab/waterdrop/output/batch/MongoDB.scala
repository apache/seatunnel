package io.github.interestinglab.waterdrop.output.batch

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class MongoDB extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  val confPrefix = "writeconfig."

  var writeConfig: WriteConfig = _

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    TypesafeConfigUtils.hasSubConfig(config, confPrefix) match {
      case true => {
        val read = TypesafeConfigUtils.extractSubConfig(config, confPrefix, false)
        read.hasPath("uri") && read.hasPath("database") && read.hasPath("collection") match {
          case true => (true, "")
          case false =>
            (false, "please specify [writeconfig.uri] and [writeconfig.database] and [writeconfig.collection]")
        }
      }
      case false => (false, "please specify [writeconfig] ")
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
    writeConfig = WriteConfig(map)
  }

  override def process(df: Dataset[Row]): Unit = {
    MongoSpark.save(df, writeConfig)
  }
}

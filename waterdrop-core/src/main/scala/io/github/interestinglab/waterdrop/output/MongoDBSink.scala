package io.github.interestinglab.waterdrop.output

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.{BaseStructuredStreamingOutput, Plugin}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}

import scala.collection.JavaConversions._

class MongoDBSink extends ForeachWriter[Row]  with BaseStructuredStreamingOutput{

  var config: Config = ConfigFactory.empty()

  val confPrefix = "writeconfig"
  val outConfPrefix = "outconfig"

  var writeConfig :WriteConfig = _
  var options = new collection.mutable.HashMap[String, String]

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }


  override def checkConfig(): (Boolean, String) = {

    config.hasPath(confPrefix)  match {
      case true => {
        val read = config.getConfig(confPrefix)
        read.hasPath("uri") && read.hasPath("database") && read.hasPath("collection") match {
          case true => (true, "")
          case false => (false, "please specify [writeconfig.uri] and [writeconfig.database] and [writeconfig.collection]")
        }
      }
      case false => (false, "please specify [writeconfig] ")
    }

    config.hasPath(outConfPrefix)  match {
      case true => {
        val outConf = config.getConfig(outConfPrefix)
        outConf.hasPath("outputMode") match {
          case true => (true, "")
          case false => (false, "please specify [outputMode] ")
        }
      }
      case false => (false, "please specify [outconfig] ")
    }

  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val map = new collection.mutable.HashMap[String, String]
    config
      .getConfig(confPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        map.put(key, value)
      })
    config
      .getConfig(outConfPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        options.put(key, value)
      })
    writeConfig = WriteConfig(map)
  }


  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(value: Row): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val rows = List[Row](value)
    val dataSet = sparkSession.createDataset(rows)
    MongoSpark.save(dataSet,writeConfig)
  }

  override def close(errorOrNull: Throwable): Unit = {}

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    df.writeStream
      .foreach(this)
      .options(options)
  }
}

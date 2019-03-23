package io.github.interestinglab.waterdrop.input.structuredstreaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingInput
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class KafkaStream extends BaseStructuredStreamingInput {
  var config: Config = ConfigFactory.empty()
  var schema = new StructType()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  // kafka consumer configuration : http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  val consumerPrefix = "consumer"

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("consumer.bootstrap.servers") && config.hasPath("topics") match {
      case true => (true, "")
      case false => (false, "please specify [consumer.bootstrap.servers] and [topics] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit ={
    config.hasPath("schema") match {
      case true => {
        val schemaJson = JSON.parseObject(config.getString("schema"))
        schema = SparkSturctTypeUtil.getStructType(schema,schemaJson)
      }
      case false =>{}
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val topics = config.getString("topics")
    val consumerConfig = config.getConfig(consumerPrefix)
    val kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
    var dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("consumer.bootstrap.servers"))
      .option("subscribe", topics)
      .options(kafkaParams)
      .load()
    if (schema.size > 0){
      var tmpDf = dataFrame.withColumn(RowConstant.TMP, from_json(col("value").cast(DataTypes.StringType), schema))
      schema.map { field =>
        tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
      }
      dataFrame = tmpDf.drop(RowConstant.TMP)
    }
    dataFrame
  }
}

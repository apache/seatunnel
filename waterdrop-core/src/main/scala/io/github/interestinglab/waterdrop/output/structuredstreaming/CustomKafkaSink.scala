package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import io.github.interestinglab.waterdrop.output.KafkaProducerUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class CustomKafkaSink extends BaseStructuredStreamingOutput {
  var config = ConfigFactory.empty()
  val producerPrefix = "producer"
  val outputOptionPrefix = "output.option"
  var options = new collection.mutable.HashMap[String, String]
  var kafkaSink: Broadcast[KafkaProducerUtil] = _
  var topic: String = _

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {
    val producerConfig = config.getConfig(producerPrefix)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => (true, "")
      case false => (false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    topic = config.getString("topic")
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "outputMode" -> "Append",
        "trigger_type" -> "default",
        producerPrefix + ".retries" -> 2,
        producerPrefix + ".acks" -> 1,
        producerPrefix + ".buffer.memory" -> 33554432,
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )
    config.withFallback(defaultConfig)
    val props = new Properties()
    config
      .getConfig(producerPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        props.put(key, value)
      })

    println("[INFO] Kafka Output properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    })

    kafkaSink = spark.sparkContext.broadcast(KafkaProducerUtil(props))

    config.hasPath(outputOptionPrefix) match {
      case true => {
        config
          .getConfig(outputOptionPrefix)
          .entrySet()
          .foreach(entry => {
            val key = entry.getKey
            val value = String.valueOf(entry.getValue.unwrapped())
            options.put(key, value)
          })
      }
      case false => {}
    }
  }

  /**
   * Things to do before process.
   **/
  override def open(partitionId: Long, epochId: Long): Boolean = true

  /**
   * Things to do with each Row.
   **/
  override def process(row: Row): Unit = {
    val json = new JSONObject()
    row.schema.fieldNames
      .foreach(field => json.put(field, row.getAs(field)))
    kafkaSink.value.send(topic, json.toJSONString)
  }

  /**
   * Things to do after process.
   **/
  override def close(errorOrNull: Throwable): Unit = {}

  /**
   * Waterdrop Structured Streaming process.
   **/
  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    var writer = df.writeStream
      .foreach(this)
      .options(options)
    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config, writer)
  }

}

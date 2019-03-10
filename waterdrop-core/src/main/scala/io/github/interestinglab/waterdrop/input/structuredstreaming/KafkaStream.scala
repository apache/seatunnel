package io.github.interestinglab.waterdrop.input.structuredstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class KafkaStream extends BaseStructuredStreamingInput {
  var config: Config = ConfigFactory.empty()

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
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("consumer.bootstrap.servers"))
      .option("subscribe", config.getString("topics"))
      .options(kafkaParams)
      .load()
  }

}

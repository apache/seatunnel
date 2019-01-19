package io.github.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.JavaConversions._

class KafkaStructuredStream extends BaseStructuredStreamingInput {
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

    val consumerConfig = config.getConfig(consumerPrefix)

    config.hasPath("kafka.bootstrap.servers") match {
      case true => {
        consumerConfig.hasPath("subscribe") ||
          consumerConfig.hasPath("subscribePattern") ||
          consumerConfig.hasPath("assign") match {
          case true => (true, "")
          case false =>
            (false, "please specify [subscribe or subscribePattern or assign] as non-empty string")
        }
      }
      case false => (false, "please specify [kafka.bootstrap.servers] as non-empty string")
    }
  }


  override def getDataset(spark: SparkSession): Dataset[Row] = {

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
      .options(kafkaParams)
      .load()
  }

}

package io.github.interestinglab.waterdrop.output.batch

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.UserRuntimeException
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.output.utils.KafkaProducerUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Kafka extends BaseOutput {

  val producerPrefix = "producer."

  var kafkaSink: Option[Broadcast[KafkaProducerUtil]] = None

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

    val producerConfig = TypesafeConfigUtils.extractSubConfig(config, producerPrefix, false)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => (true, "")
      case false => (false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "serializer" -> "json",
        producerPrefix + "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )

    config = config.withFallback(defaultConfig)

    val props = new Properties()
    TypesafeConfigUtils
      .extractSubConfig(config, producerPrefix, false)
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

    kafkaSink = Some(spark.sparkContext.broadcast(KafkaProducerUtil(props)))
  }

  override def process(df: Dataset[Row]) {

    val topic = config.getString("topic")
    config.getString("serializer") match {
      case "text" => {
        if (df.schema.size != 1) {
          throw new UserRuntimeException(
            s"Text data source supports only a single column," +
              s" and you have ${df.schema.size} columns.")
        } else {
          df.foreach { row =>
            kafkaSink.foreach { ks =>
              ks.value.send(topic, row.getAs[String](0))
            }
          }
        }
      }
      case _ => {
        val dataSet = df.toJSON
        dataSet.foreach { row =>
          kafkaSink.foreach { ks =>
            ks.value.send(topic, row)
          }
        }
      }
    }
  }
}

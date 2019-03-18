package io.github.interestinglab.waterdrop.output

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Kafka extends BaseOutput {

  val producerPrefix = "producer"

  var kafkaSink: Option[Broadcast[KafkaSink]] = None

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
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
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

    kafkaSink = Some(spark.sparkContext.broadcast(KafkaSink(props)))
  }

  override def process(df: Dataset[Row]) {

    val dataSet = df.toJSON
    dataSet.foreach { row =>
      kafkaSink.foreach { ks =>
        ks.value.send(config.getString("topic"), row)
      }
    }
  }

}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit =
    producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}

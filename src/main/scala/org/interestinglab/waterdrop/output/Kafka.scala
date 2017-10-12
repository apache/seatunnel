package org.interestinglab.waterdrop.output

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConversions._

class Kafka(var config: Config) extends BaseOutput(config) {

  val producerPrefix = "producer"

  var kafkaSink: Option[Broadcast[KafkaSink]] = None

  override def checkConfig(): (Boolean, String) = {

    val producerConfig = config.getConfig(producerPrefix)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => (true, "")
      case false => (false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "serializer" -> "json",
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )

    config = config.withFallback(defaultConfig)

    val props = new Properties()
    config.getConfig(producerPrefix).entrySet().foreach(entry => {
      val key = entry.getKey
      val value = String.valueOf(entry.getValue.unwrapped())
      props.put(key, value)
    })

    println("[INFO] Kafka Output properties: ")
    props.foreach(entry => {
      println("[INFO] \t" + entry._1 + " = " + entry._2)
    })

    kafkaSink = Some(ssc.sparkContext.broadcast(KafkaSink(props)))
  }

  override def process(df: DataFrame) {

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

package org.interestinglab.waterdrop.output

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.broadcast.Broadcast
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class Kafka(config: Config) extends BaseOutput(config) {

  val props = new Properties()
  var topic = ""
  var kafkaSink: Option[Broadcast[KafkaSink]] = None

  def checkConfig(): (Boolean, String) = {
    // TODO
    (true, "")
  }

  def prepare(ssc: StreamingContext) {
    val producerConf = config.getConfig("producer")
    this.topic = config.getString("topic")
    props.put("acks", producerConf.getString("acks"))
    props.put("bootstrap.servers", producerConf.getString("bootstrap.servers"))
    props.put("retries", producerConf.getString("retries"))
    props.put("retry.backoff.ms", producerConf.getString("retry.backoff.ms"))
    props.put("batch.size", producerConf.getString("batch.size"))
    props.put("send.buffer.bytes", producerConf.getString("send.buffer.bytes"))
    props.put("max.in.flight.requests.per.connection", producerConf.getString("max.in.flight.requests.per.connection"))
    props.put("linger.ms", producerConf.getString("linger.ms"))
    props.put("buffer.memory", producerConf.getString("buffer.memory"))
    props.put("key.serializer", producerConf.getString("key.serializer"))
    props.put("value.serializer", producerConf.getString("value.serializer"))
    props.put("compression.type", producerConf.getString("compression.type"))
    props.put("max.request.size", producerConf.getString("max.request.size"))

    kafkaSink = Some(ssc.sparkContext.broadcast(KafkaSink(props)))
  }

  def process(df: DataFrame) {

    val dataSet = df.toJSON
    dataSet.foreach { row =>
      kafkaSink.foreach { ks =>
        ks.value.send(this.topic, row)
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

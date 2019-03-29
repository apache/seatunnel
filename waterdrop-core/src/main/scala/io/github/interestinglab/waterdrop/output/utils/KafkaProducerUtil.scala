package io.github.interestinglab.waterdrop.output.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerUtil(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit =
    producer.send(new ProducerRecord(topic, value))
}

object KafkaProducerUtil {
  def apply(config: Properties): KafkaProducerUtil = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaProducerUtil(f)
  }
}

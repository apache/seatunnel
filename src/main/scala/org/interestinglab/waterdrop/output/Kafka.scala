package org.interestinglab.waterdrop.output

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.broadcast.Broadcast
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.Event
import org.json4s.jackson


class Kafka(config : Config) extends BaseOutput(config) {

    val props = new Properties()

    var kafkaSink : Option[Broadcast[KafkaSink]] = None

    def checkConfig() : (Boolean, String) = {
        // TODO
        (true, "")
    }

    def prepare(ssc : StreamingContext) {
        val producerConf = config.getConfig("producer")
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

    def process(eventIter : Iterator[Event]) {
        for (event <- eventIter) {

            implicit val formats = org.json4s.DefaultFormats
            val message = jackson.Serialization.write(event.toMap)

            kafkaSink.foreach { ks =>
                ks.value.send(config.getString("topic"), message)
            }
        }    
    }
}


class KafkaSink(createProducer : () => KafkaProducer[String, String]) extends Serializable {

    lazy val producer = createProducer()

    def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}


object KafkaSink {
    def apply(config : Properties): KafkaSink = {
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

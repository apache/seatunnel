package io.github.interestinglab.waterdrop.input.structuredstreaming

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingInput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}

import scala.collection.JavaConversions._

class KafkaStream extends BaseStructuredStreamingInput {
  var config: Config = ConfigFactory.empty()
  var schema = new StructType()
  var topics: String = _
  var consumer: KafkaConsumer[String, String] = _
  var kafkaParams: Map[String, String] = _
  val offsetMeta = new util.HashMap[String, util.HashMap[String, Long]]()
  val pollDuration = 100
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  // kafka consumer configuration : http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  val consumerPrefix = "consumer."

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("consumer.bootstrap.servers") && config.hasPath("topics") &&
      config.hasPath("consumer.group.id") match {
      case true => (true, "")
      case false =>
        (false, "please specify [consumer.bootstrap.servers] and [topics] and [consumer.group.id] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    topics = config.getString("topics")

    config.hasPath("schema") match {
      case true => {
        val schemaJson = JSON.parseObject(config.getString("schema"))
        schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)
      }
      case false => {}
    }

    //offset异步回写
    spark.streams.addListener(new StreamingQueryListener() {

      override def onQueryStarted(event: QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        //这个是kafkaInput的正则匹配
        val pattern = Pattern.compile(".+\\[Subscribe\\[(.+)\\]\\]");
        val sources = event.progress.sources
        sources.foreach(source => {
          val matcher = pattern.matcher(source.description)
          if (matcher.find() && topics.equals(matcher.group(1))) {
            //每个input负责监控自己的topic
            val endOffset = JSON.parseObject(source.endOffset)
            endOffset
              .keySet()
              .foreach(topic => {
                val partitionToOffset = endOffset.getJSONObject(topic)
                partitionToOffset
                  .keySet()
                  .foreach(partition => {
                    val offset = partitionToOffset.getLong(partition)
                    val topicPartition = new TopicPartition(topic, Integer.parseInt(partition))
                    consumer.poll(Duration.ofMillis(pollDuration))
                    consumer.seek(topicPartition, offset)
                  })
              })
          }
        })
        consumer.commitSync()
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        consumer.close()
      }
    })

    val consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
    kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    val topicList = initConsumer()

    //如果用户选择offset从broker获取
    config.hasPath("offset.location") match {
      case true =>
        config.getString("offset.location").equals("broker") match {
          case true => {
            setOffsetMeta(topicList)
            kafkaParams += ("startingOffsets" -> JSON.toJSONString(offsetMeta, SerializerFeature.WriteMapNullValue))
          }
          case false => {}
        }
      case false => {}
    }

    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    var dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("consumer.bootstrap.servers"))
      .option("subscribe", topics)
      .options(kafkaParams)
      .load()
    if (schema.size > 0) {
      var tmpDf = dataFrame.withColumn(RowConstant.TMP, from_json(col("value").cast(DataTypes.StringType), schema))
      schema.map { field =>
        tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
      }
      dataFrame = tmpDf.drop(RowConstant.TMP)
    }
    dataFrame
  }

  private def initConsumer(): util.ArrayList[String] = {
    val props = new Properties()
    props.put("bootstrap.servers", config.getString("consumer.bootstrap.servers"))
    props.put("group.id", config.getString("consumer.group.id"))
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumer = new KafkaConsumer[String, String](props)
    val topicList = new util.ArrayList[String]()
    topics.split(",").foreach(topicList.add(_))
    consumer.subscribe(topicList)
    topicList
  }

  private def setOffsetMeta(topicList: util.ArrayList[String]): Unit = {
    topicList.foreach(topic => {
      val partition2offset = new util.HashMap[String, Long]()
      val topicInfo = consumer.partitionsFor(topic)
      topicInfo.foreach(info => {
        val topicPartition = new TopicPartition(topic, info.partition())
        val metadata = consumer.committed(topicPartition)
        partition2offset.put(String.valueOf(info.partition()), metadata.offset())
      })
      offsetMeta.put(topic, partition2offset)
    })
  }
}

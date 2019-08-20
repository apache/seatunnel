package io.github.interestinglab.waterdrop.spark.source

import java.util.Properties
import io.github.interestinglab.waterdrop.common.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingSource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  HasOffsetRanges,
  KafkaUtils,
  LocationStrategies,
  OffsetRange
}

import scala.collection.JavaConversions._

class KafkaStream extends SparkStreamingSource[(String, String)] {

  private var schema: StructType = _

  private val kafkaParams = new Properties()

  private var offsetRanges: Array[OffsetRange] = _

  private var inputDStream: InputDStream[ConsumerRecord[String, String]] = _

  private val consumerPrefix = "consumer."

  private var topics: Set[String] = _

  override def prepare(): Unit = {

    schema = StructType(
      Array(StructField("topic", DataTypes.StringType),
            StructField("raw_message", DataTypes.StringType)))

    topics = config.getString("topics").split(",").toSet
    val consumerConfig =
      TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
    consumerConfig.entrySet.foreach(entry => {
      val key = entry.getKey
      val value = entry.getValue.unwrapped
      kafkaParams.put(key, String.valueOf(value))
    })
  }

  override def rdd2dataset(sparkSession: SparkSession,
                           rdd: RDD[(String, String)]): Dataset[Row] = {
    val value = rdd.map(record => Row(record._1, record._2))
    sparkSession.createDataFrame(value, schema)
  }

  override def getData(env: SparkEnvironment): DStream[(String, String)] = {

    inputDStream = KafkaUtils.createDirectStream(
      env.getStreamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams))

    inputDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(record => {
        val topic = record.topic()
        val value = record.value()
        (topic, value)
      })
    }

  }

  override def checkConfig(): CheckResult = new CheckResult(true, "")
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.spark.source

import java.util.Properties

import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.common.config.CheckConfigUtil.check
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.stream.SparkStreamingSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.slf4j.LoggerFactory

class KafkaStream extends SparkStreamingSource[(String, String)] {

  private val LOGGER = LoggerFactory.getLogger(classOf[KafkaStream])

  private var schema: StructType = _

  private val kafkaParams = new Properties()

  private var offsetRanges: Array[OffsetRange] = _

  private var inputDStream: InputDStream[ConsumerRecord[String, String]] = _

  private val consumerPrefix = "consumer."

  private var topics: Set[String] = _

  override def prepare(env: SparkEnvironment): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        consumerPrefix + "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + "enable.auto.commit" -> false))

    config = config.withFallback(defaultConfig)
    schema = StructType(
      Array(
        StructField("topic", DataTypes.StringType),
        StructField("raw_message", DataTypes.StringType)))

    topics = config.getString("topics").split(",").toSet
    val consumerConfig =
      TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
    consumerConfig.entrySet.foreach(entry => {
      val key = entry.getKey
      val value = entry.getValue.unwrapped
      kafkaParams.put(key, String.valueOf(value))
    })

    LOGGER.info("Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      LOGGER.info("\t" + key + " = " + value)
    }
  }

  override def rdd2dataset(sparkSession: SparkSession, rdd: RDD[(String, String)]): Dataset[Row] = {
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

  override def checkConfig(): CheckResult = {
    val checkResult = check(config, "topics")
    if (checkResult.isSuccess) {
      val consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
      check(consumerConfig, "group.id")
    } else {
      checkResult
    }
  }

  override def afterOutput(): Unit = {
    inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    for (offsets <- offsetRanges) {
      val fromOffset = offsets.fromOffset
      val untilOffset = offsets.untilOffset
      if (untilOffset != fromOffset) {
        LOGGER.info(
          s"complete consume topic: ${offsets.topic} partition:" +
            s"${offsets.partition} from ${fromOffset} until ${untilOffset}")
      }
    }
  }
}

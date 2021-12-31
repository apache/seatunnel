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
package org.apache.seatunnel.spark.sink

import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

import java.util.Properties
import scala.collection.JavaConversions._

class Kafka extends SparkBatchSink with Logging {

  val producerPrefix = "producer."

  var kafkaSink: Option[Broadcast[KafkaProducerUtil]] = None

  override def checkConfig(): CheckResult = {

    val producerConfig = TypesafeConfigUtils.extractSubConfig(config, producerPrefix, false)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => new CheckResult(true, "")
      case false =>
        new CheckResult(false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "format" -> "json",
        producerPrefix + "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"))

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

    log.info("Kafka Output properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      log.info(key + " = " + value)
    })

    kafkaSink = Some(env.getSparkSession.sparkContext.broadcast(KafkaProducerUtil(props)))
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {

    val topic = config.getString("topic")
    var format = config.getString("format")
    if (config.hasPath("serializer")) {
      format = config.getString("serializer")
    }
    format match {
      case "text" => {
        if (df.schema.size != 1) {
          throw new Exception(
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

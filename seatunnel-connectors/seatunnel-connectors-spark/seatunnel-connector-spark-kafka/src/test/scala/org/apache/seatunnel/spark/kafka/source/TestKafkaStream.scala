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

package org.apache.seatunnel.spark.kafka.source

import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.util.{Failure, Success}

import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.seatunnel.common.constants.JobMode
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.kafka.TestKafka
import org.scalatest.BeforeAndAfterEach

class TestKafkaStream extends TestKafka with BeforeAndAfterEach {
  var testTopic: String = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testTopic = RandomStringUtils.randomAlphabetic(6)
    kafka.createTopic(testTopic, 1) match {
      case Success(_) =>
      case Failure(e) =>
        e.printStackTrace()
        sys.exit(-1)
    }
  }

  "KafkaStream" should "work" in {
    val config = prepareDefaultConfig()
    val environment = new SparkEnvironment
    environment.setJobMode(JobMode.STREAMING)
      .setConfig(config.getConfig("env"))
      .prepare
    val ssc = environment.getStreamingContext

    val kafkaStream = new KafkaStream
    kafkaStream.setConfig(config.getConfigList("source").get(0))
    kafkaStream.prepare(environment)
    kafkaStream.start(environment, _ => ssc.stop(stopSparkContext = true, stopGracefully = false))
    kafka.mock(testTopic, v = "row")

    ssc.start()
    if (!ssc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(10))) {
      ssc.stop()
      fail(s"it be able to read from topic[${testTopic}].")
    }
  }

  it should "fail with exception caught due to invalid config" in {
    val mConfig = Map("consumer." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka.getBootstrapServers).asJava
    val config = ConfigFactory.parseMap(mConfig)
    val kafkaStream = new KafkaStream
    kafkaStream.setConfig(config)

    if (kafkaStream.checkConfig().isSuccess) {
      fail("checkConfig should not succeed, exception is expected.")
    }
  }

  private def prepareDefaultConfig() = {
    val mConfig = Map(
      "env" -> Map("spark.master" -> "local[4]").asJava,
      "source" -> List(Map(
        "plugin_name" -> "KafkaStream",
        "consumer." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka.getBootstrapServers,
        "consumer." + ConsumerConfig.GROUP_ID_CONFIG -> "group-1",
        "topics" -> testTopic).asJava).asJava,
      "sink" -> List().asJava)
    ConfigFactory.parseMap(mConfig)
  }

  override def afterEach(): Unit = {
    try {
      kafka.deleteTopics(List(testTopic))
    } finally {
      super.afterEach()
    }
  }
}

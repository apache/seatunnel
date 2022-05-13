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

package org.apache.seatunnel.spark.kafka

import java.net.ServerSocket
import java.time.Duration

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.slf4j.{Logger, LoggerFactory}

trait TestKafka extends FlatSpec with BeforeAndAfterAll {
  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  var zookeeper: EmbeddedZookeeper = _
  var kafka: EmbeddedKafka = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val zkPort = allocatePort() match {
      case Success(result) => result
      case Failure(e) =>
        LOG.error("Can not allocate port for zk, ", e)
        sys.exit(-1)
    }
    zookeeper = new EmbeddedZookeeper(port = zkPort)
    zookeeper.start()

    val kafkaPort = allocatePort() match {
      case Success(result) => result
      case Failure(e) =>
        LOG.error("Can not allocate port for kafka, ", e)
        sys.exit(-1)
    }
    kafka = new EmbeddedKafka(port = kafkaPort, zkStr = zookeeper.getConnectString)
    kafka.start()

    check()
  }

  def check(): Unit = {
    val checkTopic = "check"
    kafka.createTopic(checkTopic, 1) match {
      case Success(_) =>
      case Failure(e) =>
        e.printStackTrace()
        sys.exit(-1)
    }

    val configs = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka.getBootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> "check",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName)
    val consumer = new KafkaConsumer(configs)
    consumer.subscribe(List(checkTopic))

    LOG.info("Start checking...")
    var check = false
    Breaks.breakable(for (_ <- 0 until 10) {
      kafka.mock(checkTopic, v = "check")
      val recs = consumer.poll(Duration.ofSeconds(5))
      if (!recs.isEmpty) {
        check = true
        Breaks.break()
      }
    })
    assert(check, "Kafka server startup failed.")
    consumer.close()
    kafka.deleteTopics(List(checkTopic))
    LOG.info("Kafka server check success.")
  }

  override def afterAll(): Unit = {
    try {
      kafka.stop()
      zookeeper.stop()
    } finally super.afterAll()
  }

  def allocatePort(): Try[Int] = Try {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}

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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.reflect.io.{Directory, File}
import scala.util.Try

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.admin.{AdminClient, DeleteTopicsOptions, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

class EmbeddedKafka(val host: String = "127.0.0.1", val port: Int, val zkStr: String) {
  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  var kafka: KafkaServerStartable = _
  val tmpDir: String = sys.props("java.io.tmpdir")
  val dataDir: Directory = Directory.makeTemp(prefix = "kafka-", dir = File(tmpDir).jfile)

  var adminClient: AdminClient = _
  var producer: KafkaProducer[String, String] = _

  def start(): Unit = {
    val props = Map(
      KafkaConfig.ZkConnectProp -> zkStr,
      KafkaConfig.HostNameProp -> host,
      KafkaConfig.PortProp -> port,
      KafkaConfig.LogDirProp -> dataDir.path,
      KafkaConfig.OffsetsTopicReplicationFactorProp -> 1.toShort,
      KafkaConfig.AutoCreateTopicsEnableProp -> false,
      // if delete.topic.enable set to true, when we delete topic before kafka shutdown, is maybe happen on windows. https://issues.apache.org/jira/browse/KAFKA-9458
      KafkaConfig.DeleteTopicEnableProp -> false)
    val config = new KafkaConfig(props)
    kafka = new KafkaServerStartable(config)
    kafka.startup()
    init()
  }

  def init(): Unit = {
    val adminClientConfig = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"$host:$port")
    adminClient = AdminClient.create(adminClientConfig)

    val producerConfigs = Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"$host:$port")
    producer = new KafkaProducer(producerConfigs, new StringSerializer, new StringSerializer)
  }

  def createTopic(topic: String, partitions: Int): Try[_] = Try {
    val newTopic = new NewTopic(topic, partitions, 1)
    val result = adminClient.createTopics(List(newTopic))
    LOG.info("start create topic {}...", topic)
    result.all().get(999, TimeUnit.SECONDS)
  }

  def deleteTopics(topic: List[String]): Unit = {
    adminClient.deleteTopics(topic, new DeleteTopicsOptions().timeoutMs(1000))
  }

  def mock(topic: String, k: String = "", v: String): Unit = Try {
    val rec = new ProducerRecord(topic, k, v)
    producer.send(rec).get
  }

  def getBootstrapServers: String = s"$host:$port"

  def stop(): Unit = {
    kafka.shutdown()
    dataDir.deleteRecursively()
  }
}

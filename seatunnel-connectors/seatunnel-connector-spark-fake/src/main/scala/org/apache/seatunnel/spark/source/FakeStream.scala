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

import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.{Config, ConfigFactory}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.stream.SparkStreamingSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import java.security.SecureRandom

import org.apache.seatunnel.common.config.CheckConfigUtil.check

import scala.collection.JavaConversions._

class FakeStream extends SparkStreamingSource[String] {

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "rate" -> 1 // rate per second, X records/sec
      ))
    config = config.withFallback(defaultConfig)
  }

  override def getData(env: SparkEnvironment): DStream[String] = {
    val receiverInputDStream = env.getStreamingContext.receiverStream(new FakeReceiver(config))
    receiverInputDStream
  }

  override def rdd2dataset(sparkSession: SparkSession, rdd: RDD[String]): Dataset[Row] = {
    val rowsRDD = rdd.map(element => {
      RowFactory.create(element)
    })
    val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
    sparkSession.createDataFrame(rowsRDD, schema)
  }

  override def checkConfig(): CheckResult = {
    check(config, "content")
  }
}

private class FakeReceiver(config: Config)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  val secRandom = new SecureRandom()

  def generateData(): String = {
    val contentList = config.getStringList("content")
    val n = secRandom.nextInt(contentList.length)
    contentList.get(n)
  }

  def onStart(): Unit = {
    // Start the thread that receives data over a connection

    new Thread("FakeReceiver Source") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(): Unit = {
    while (!isStopped()) {

      store(generateData())
      Thread.sleep((1000.toDouble / config.getInt("rate")).toInt)
    }
  }
}

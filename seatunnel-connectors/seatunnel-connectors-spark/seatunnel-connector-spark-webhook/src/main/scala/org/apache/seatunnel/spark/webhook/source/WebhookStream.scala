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

package org.apache.seatunnel.spark.webhook.source

import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.stream.SparkStreamingSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.streaming.dstream.DStream

class WebhookStream extends SparkStreamingSource[String] {

  // Create server
  var server: JettyServer = null

  override def start(env: SparkEnvironment, handler: Dataset[Row] => Unit): Unit = {
    var spark = env.getSparkSession
    // Create HTTP Server and start streaming
    implicit val sqlContext: SQLContext = spark.sqlContext

    var port = if (config.hasPath("port")) config.getInt("port") else 9999
    var baseUrl = if (config.hasPath("path")) config.getString("path") else "/"

    this.server = new JettyServer(port, baseUrl)
    val query = this.server
      .toDF
      .writeStream
      .foreachBatch((batch, batchId) => {
        handler(batch)
      })
      .start()

    query.awaitTermination()
  }

  def stop(): Unit = {
    if (this.server != null) {
      this.server.stop()
    }
  }

  override def rdd2dataset(sparkSession: SparkSession, rdd: RDD[String]): Dataset[Row] = { null }

  override def getData(env: SparkEnvironment): DStream[String] = { null }
}

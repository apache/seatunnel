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
package org.apache.seatunnel.spark.stream

import org.apache.seatunnel.spark.{BaseSparkSource, SparkEnvironment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

/**
 * a SparkStreamingSource plugin will read data from other system
 * using Spark Streaming API.
 */
trait SparkStreamingSource[T] extends BaseSparkSource[DStream[T]] {

  def beforeOutput(): Unit = {}

  def afterOutput(): Unit = {}

  def rdd2dataset(sparkSession: SparkSession, rdd: RDD[T]): Dataset[Row]

  def start(env: SparkEnvironment, handler: Dataset[Row] => Unit): Unit = {
    getData(env).foreachRDD(rdd => {
      val dataset = rdd2dataset(env.getSparkSession, rdd)
      handler(dataset)
    })
  }

}

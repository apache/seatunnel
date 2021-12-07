/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.apis

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * A Java-friendly version of [[BaseStreamingInput]] that returns
 * [[BaseJavaStreamingInput]]s and works with JavaStreamingContext, JavaRDD instead of Scala ones.
 * */
abstract class BaseJavaStreamingInput[T] extends BaseStreamingInput[T] {

  def rdd2dataset(spark: SparkSession, rdd: RDD[T]): Dataset[Row] = {
    javaRdd2dataset(spark, rdd.toJavaRDD())
  }

  def getDStream(ssc: StreamingContext): DStream[T] = {
    getJavaDstream(new JavaStreamingContext(ssc)).dstream
  }

  /**
   * Create spark javaDStream from data source, you can specify type parameter.
   * */
  def getJavaDstream(jssc: JavaStreamingContext): JavaDStream[T]

  /**
   * This must be implemented to convert JavaRDD[T] to Dataset[Row] for later processing
   * */
  def javaRdd2dataset(sparkSession: SparkSession, javaRDD: JavaRDD[T]): Dataset[Row]
}

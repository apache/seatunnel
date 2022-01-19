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

import com.redislabs.provider.redis.{toRedisContext, RedisConfig, RedisEndpoint}
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Redis extends SparkBatchSource {
  val defaultPort: Int = 6379
  val defaultDb: Int = 0
  val defaultTimeout: Int = 2000
  val defaultPartition: Int = 3

  override def checkConfig(): CheckResult = {
    val hasTableName = config.hasPath("result_table_name") || config.hasPath("table_name")
    val hasRedisHost = config.hasPath("host")
    val hasKeys = config.hasPath("key_pattern")
    val hasRedisPassword = config.hasPath("auth")

    config match {
      case _ if !hasTableName =>
        CheckResult.error("please specify [result_table_name] as non-empty string")
      case _ if !hasRedisHost => CheckResult.error("please specify [host] as non-empty string")
      case _ if !hasRedisPassword =>
        CheckResult.error("please specify [auth] as non-empty string")
      case _ if !hasKeys =>
        CheckResult.error(
          "please specify [key_pattern] as non-empty string, multiple key patterns separated by ','")
      case _ => CheckResult.success()
    }
  }

  /**
   * Parameter item setting
   *
   * @param env Spark environment
   */
  override def prepare(env: SparkEnvironment): Unit = {}

  /**
   * Read the data in redis and convert it into dataframe
   *
   * @param env Spark environment
   * @return Return the input dataset
   */
  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val spark = env.getSparkSession

    var regTable = ""
    if (config.hasPath("result_table_name")) {
      regTable = config.getString("result_table_name")
    } else {
      regTable = config.getString("table_name")
    }

    val auth = config.getString("auth")

    val host = config.getString("host")

    var port = defaultPort
    if (config.hasPath("port")) {
      port = config.getInt("port")
    }

    var timeout = defaultTimeout
    if (config.hasPath("timeout")) {
      timeout = config.getInt("timeout")
    }

    val keyPattern = config.getString("key_pattern")

    var partition = defaultPartition
    if (config.hasPath("partition")) {
      partition = config.getInt("partition")
    }

    var dbNum = defaultDb
    if (config.hasPath("db_num")) {
      dbNum = config.getInt("db_num")
    }

    // Get data from redis through keys and combine it into a dataset
    val redisConfig = new RedisConfig(RedisEndpoint(
      host = host,
      port = port,
      auth = auth,
      dbNum = dbNum,
      timeout = timeout))
    val stringRDD = spark.sparkContext.fromRedisKV(keyPattern, partition)(redisConfig = redisConfig)
    import spark.implicits._
    val ds = stringRDD.toDF("raw_key", "raw_message")
    ds.createOrReplaceTempView(s"$regTable")
    ds
  }
}

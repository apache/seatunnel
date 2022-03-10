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

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, toRedisContext}
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.seatunnel.spark.common.Constants.{AUTH, DATA_TYPE, DB_NUM, DEFAULT_AUTH, DEFAULT_DATA_TYPE, DEFAULT_DB_NUM, DEFAULT_HOST, DEFAULT_PARTITION_NUM, DEFAULT_PORT, DEFAULT_TIMEOUT, HOST, KEYS_OR_KEY_PATTERN, PARTITION_NUM, PORT, TIMEOUT}
import org.apache.seatunnel.spark.common.RedisDataType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Redis extends SparkBatchSource {

  var redisDataType: RedisDataType.Value = _

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, HOST, KEYS_OR_KEY_PATTERN)
  }

  /**
   * Parameter item setting
   *
   * @param env Spark environment
   */
  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        HOST -> DEFAULT_HOST,
        PORT -> DEFAULT_PORT,
        AUTH -> DEFAULT_AUTH,
        DB_NUM -> DEFAULT_DB_NUM,
        DATA_TYPE -> DEFAULT_DATA_TYPE,
        PARTITION_NUM -> DEFAULT_PARTITION_NUM,
        TIMEOUT -> DEFAULT_TIMEOUT
      ))
    config = config.withFallback(defaultConfig)
  }

  /**
   * Read the data in redis and convert it into dataframe
   *
   * @param env Spark environment
   * @return Return the input dataset
   */
  override def getData(env: SparkEnvironment): Dataset[Row] = {
    // Get data from redis through keys and combine it into a dataset
    val redisConfigs = new RedisConfig(RedisEndpoint(
      host = config.getString(HOST),
      port = config.getInt(PORT),
      auth = config.getString(AUTH),
      dbNum = config.getInt(DB_NUM),
      timeout = config.getInt(TIMEOUT)
    ))

    redisDataType = RedisDataType.withName(config.getString(DATA_TYPE).toUpperCase)
    val keysOrKeyPattern = config.getString(KEYS_OR_KEY_PATTERN)
    val partitionNum = config.getInt(PARTITION_NUM)

    val spark = env.getSparkSession
    implicit val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    var ds = spark.emptyDataFrame
    redisDataType match {
      case RedisDataType.KV =>
        val resultRDD = dealWithKV(keysOrKeyPattern, partitionNum)(sc = sc, redisConfig = redisConfigs)
        ds = resultRDD.toDF("raw_key", "raw_message")
      case RedisDataType.HASH =>
        val resultRDD = dealWithHASH(keysOrKeyPattern, partitionNum)(sc = sc, redisConfig = redisConfigs)
        ds = resultRDD.toDF("raw_key", "raw_message")
      case RedisDataType.SET =>
        val resultRDD = dealWithSet(keysOrKeyPattern, partitionNum)(sc = sc, redisConfig = redisConfigs)
        ds = resultRDD.toDF("raw_message")
      case RedisDataType.ZSET =>
        val resultRDD = dealWithZSet(keysOrKeyPattern, partitionNum)(sc = sc, redisConfig = redisConfigs)
        ds = resultRDD.toDF("raw_message")
      case RedisDataType.LIST =>
        val resultRDD = dealWithList(keysOrKeyPattern, partitionNum)(sc = sc, redisConfig = redisConfigs)
        ds = resultRDD.toDF("raw_message")
    }

    ds
  }

  def dealWithKV(keysOrKeyPattern: String, partitionNum: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): RDD[(String, String)] = {
    sc.fromRedisKV(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
  }

  def dealWithHASH(keysOrKeyPattern: String, partitionNum: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): RDD[(String, String)] = {
    sc.fromRedisHash(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
  }

  def dealWithList(keysOrKeyPattern: String, partitionNum: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): RDD[String] = {
    sc.fromRedisList(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
  }

  def dealWithSet(keysOrKeyPattern: String, partitionNum: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): RDD[String] = {
    sc.fromRedisSet(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
  }

  def dealWithZSet(keysOrKeyPattern: String, partitionNum: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): RDD[String] = {
    sc.fromRedisZSet(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
  }

}

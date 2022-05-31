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

package org.apache.seatunnel.spark.redis.sink

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, toRedisContext}
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.redis.common.Constants.{AUTH, DATA_TYPE, DB_NUM, DEFAULT_AUTH, DEFAULT_DATA_TYPE, DEFAULT_DB_NUM, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT, DEFAULT_TTL, HASH_NAME, HOST, LIST_NAME, PORT, SET_NAME, TIMEOUT, ZSET_NAME, TTL}
import org.apache.seatunnel.spark.redis.common.RedisDataType
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Redis extends SparkBatchSink with Logging {

  var redisDataType: RedisDataType.Value = _

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val redisConfigs = new RedisConfig(RedisEndpoint(
      host = config.getString(HOST),
      port = config.getInt(PORT),
      auth = if (config.getIsNull(AUTH)) null else config.getString(AUTH),
      dbNum = config.getInt(DB_NUM),
      timeout = config.getInt(TIMEOUT)
    ))

    redisDataType = RedisDataType.withName(config.getString(DATA_TYPE))
    implicit val sc: SparkContext = env.getSparkSession.sparkContext

    redisDataType match {
      case RedisDataType.KV => dealWithKV(data, config.getInt(TTL))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.HASH => dealWithHASH(data, config.getString(HASH_NAME), config.getInt(TTL))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.SET => dealWithSet(data, config.getString(SET_NAME), config.getInt(TTL))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.ZSET => dealWithZSet(data, config.getString(ZSET_NAME), config.getInt(TTL))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.LIST => dealWithList(data, config.getString(LIST_NAME), config.getInt(TTL))(sc = sc, redisConfig = redisConfigs)
    }
  }

  override def checkConfig(): CheckResult = {
    if (config.hasPath(DATA_TYPE)) {
      val dataType = config.getString(DATA_TYPE)
      val dataTypeList = List("KV", "HASH", "SET", "ZSET", "LIST")
      val bool = dataTypeList.contains(dataType.toUpperCase)
      if (!bool) {
        CheckResult.error("Unknown redis config. data_type must be in [KV HASH SET ZSET LIST]")
      } else {
        CheckResult.success()
      }
    } else {
      CheckResult.success()
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        HOST -> DEFAULT_HOST,
        PORT -> DEFAULT_PORT,
        AUTH -> DEFAULT_AUTH,
        DB_NUM -> DEFAULT_DB_NUM,
        DATA_TYPE -> DEFAULT_DATA_TYPE,
        TIMEOUT -> DEFAULT_TIMEOUT,
        TTL -> DEFAULT_TTL
      ))
    config = config.withFallback(defaultConfig)
  }

  def dealWithKV(data: Dataset[Row], ttl: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisKV(value, ttl)(redisConfig = redisConfig)
  }

  def dealWithList(data: Dataset[Row], listName: String, ttl: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => x.getString(0))
    sc.toRedisLIST(value, listName, ttl)(redisConfig = redisConfig)
  }

  def dealWithSet(data: Dataset[Row], setName: String, ttl: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => x.getString(0))
    sc.toRedisSET(value, setName, ttl)(redisConfig = redisConfig)
  }

  def dealWithZSet(data: Dataset[Row], setName: String, ttl: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisZSET(value, setName, ttl)(redisConfig = redisConfig)
  }

  def dealWithHASH(data: Dataset[Row], hashName: String, ttl: Int)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisHASH(value, hashName, ttl)(redisConfig = redisConfig)
  }

  override def getPluginName: String = "Redis"
}

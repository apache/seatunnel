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
package org.apache.seatunnel.spark.sink

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, toRedisContext}
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable

class Redis extends SparkBatchSink with Logging {

  val redisConfig: mutable.Map[String, String] = mutable.Map()
  val redisPrefix = "redis"
  var redisSaveType: RedisSaveType.Value = _
  val redisHost = "redis_host"
  val redisPort = "redis_port"
  val redisAuth = "redis_auth"
  val redisDb = "redis_db"
  val redisTimeout = "redis_timeout"
  val REDIS_SAVE_TYPE = "redis_save_type"
  val HASH_NAME = "redis_hash_name"
  val SET_NAME = "redis_set_name"
  val ZSET_NAME = "redis_zset_name"
  val LIST_NAME = "redis_list_name"

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val redisConfigs = new RedisConfig(RedisEndpoint(
      host = config.getString(redisHost),
      port = config.getInt(redisPort),
      auth = config.getString(redisAuth),
      dbNum = config.getInt(redisDb),
      timeout = config.getInt(redisTimeout))
    )

    implicit val sc: SparkContext = env.getSparkSession.sparkContext
    redisSaveType match {
      case RedisSaveType.KV => dealWithKV(data)(sc = sc, redisConfig = redisConfigs)
      case RedisSaveType.HASH => dealWithHASH(data, redisConfig(HASH_NAME))(sc = sc, redisConfig = redisConfigs)
      case RedisSaveType.SET => dealWithSet(data, redisConfig(SET_NAME))(sc = sc, redisConfig = redisConfigs)
      case RedisSaveType.ZSET => dealWithZSet(data, redisConfig(ZSET_NAME))(sc = sc, redisConfig = redisConfigs)
      case RedisSaveType.LIST => dealWithList(data, redisConfig(LIST_NAME))(sc = sc, redisConfig = redisConfigs)
    }
  }

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "redis_host", "redis_port", "redis_save_type")

    val saveTypeList = List("KV", "HASH", "SET", "ZSET", "LIST")
    val saveType = config.getString("redis_save_type")

    val bool = saveTypeList.contains(saveType.toUpperCase)
    if (bool) {
      redisSaveType = RedisSaveType.withName(config.getString("redis_save_type"))
      CheckResult.success()
    } else {
      CheckResult.error("Unknown redis config. redis_save_type must be in [KV HASH SET ZSET LIST]")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        redisHost -> "localhost",
        redisPort -> 6379,
        redisAuth -> null,
        redisDb -> 0,
        redisTimeout -> 2000)
    )
    config = config.withFallback(defaultConfig)
  }

  def dealWithKV(data: Dataset[Row])(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisKV(value)(redisConfig = redisConfig)
  }

  def dealWithList(data: Dataset[Row], listName: String)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => x.getString(0))
    sc.toRedisLIST(value, listName)(redisConfig = redisConfig)
  }

  def dealWithSet(data: Dataset[Row], setName: String)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => x.getString(0))
    sc.toRedisSET(value, setName)(redisConfig = redisConfig)
  }

  def dealWithZSet(data: Dataset[Row], setName: String)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisZSET(value, setName)(redisConfig = redisConfig)
  }

  def dealWithHASH(data: Dataset[Row], hashName: String)(implicit sc: SparkContext, redisConfig: RedisConfig): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisHASH(value, hashName)(redisConfig = redisConfig)
  }
}

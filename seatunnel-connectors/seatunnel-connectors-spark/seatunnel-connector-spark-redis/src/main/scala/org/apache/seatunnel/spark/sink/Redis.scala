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
import org.apache.seatunnel.spark.common.Constants.{AUTH, DATA_TYPE, DB_NUM, DEFAULT_AUTH, DEFAULT_DB_NUM, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT, HASH_NAME, HOST, LIST_NAME, PORT, SET_NAME, TIMEOUT, ZSET_NAME}
import org.apache.seatunnel.spark.common.RedisDataType
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
      auth = config.getString(AUTH),
      dbNum = config.getInt(DB_NUM),
      timeout = config.getInt(TIMEOUT)
    ))

    redisDataType = RedisDataType.withName(config.getString(DATA_TYPE))
    implicit val sc: SparkContext = env.getSparkSession.sparkContext

    redisDataType match {
      case RedisDataType.KV => dealWithKV(data)(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.HASH => dealWithHASH(data, config.getString(HASH_NAME))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.SET => dealWithSet(data, config.getString(SET_NAME))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.ZSET => dealWithZSet(data, config.getString(ZSET_NAME))(sc = sc, redisConfig = redisConfigs)
      case RedisDataType.LIST => dealWithList(data, config.getString(LIST_NAME))(sc = sc, redisConfig = redisConfigs)
    }
  }

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, HOST, PORT)

    val dataType = config.getString(DATA_TYPE)
    if (dataType != null) {
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
        TIMEOUT -> DEFAULT_TIMEOUT
      ))
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

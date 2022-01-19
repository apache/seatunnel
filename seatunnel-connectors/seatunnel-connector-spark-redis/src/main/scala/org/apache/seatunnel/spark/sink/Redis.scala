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

import com.redislabs.provider.redis.toRedisContext
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable

class Redis extends SparkBatchSink with Logging {

  val redisConfig: mutable.Map[String, String] = mutable.Map()
  val redisPrefix = "redis"
  var redisSaveType: RedisSaveType.Value = _
  val HASH_NAME = "redis_hash_name"
  val SET_NAME = "redis_set_name"
  val ZSET_NAME = "redis_zset_name"
  val LIST_NAME = "redis_list_name"
  val REDIS_SAVE_TYPE = "redis_save_type"

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    implicit val sc: SparkContext = env.getSparkSession.sparkContext
    redisSaveType match {
      case RedisSaveType.KV => dealWithKV(data)
      case RedisSaveType.HASH => dealWithHASH(data, redisConfig(HASH_NAME))
      case RedisSaveType.SET => dealWithSet(data, redisConfig(SET_NAME))
      case RedisSaveType.ZSET => dealWithZSet(data, redisConfig(ZSET_NAME))
      case RedisSaveType.LIST => dealWithList(data, redisConfig(LIST_NAME))
    }
  }

  override def checkConfig(): CheckResult = {
    config.entrySet().asScala.filter(x => x.getKey.startsWith("redis")).foreach(entry => {
      if (entry.getKey.equals("redis_save_type")) {
        redisSaveType = RedisSaveType.withName(config.getString("redis_save_type"))
      }
      redisConfig.put(entry.getKey, config.getString(entry.getKey))
    })

    def checkParam(checkArr: Array[String]): CheckResult = {
      val notExistConfig: Array[String] = checkArr.filter(checkItem => !config.hasPath(checkItem))
      if (notExistConfig.isEmpty) {
        new CheckResult(true, "redis config is enough")
      } else {
        new CheckResult(false, s"redis config is not enough please check config [${notExistConfig.mkString(",")}]")
      }
    }

    val result = checkParam(Array("redis_save_type", "redis_host", "redis_port"))
    if (!result.isSuccess) {
      result
    } else {
      redisSaveType match {
        case RedisSaveType.KV => checkParam(Array())
        case RedisSaveType.HASH => checkParam(Array(HASH_NAME))
        case RedisSaveType.SET => checkParam(Array(SET_NAME))
        case RedisSaveType.ZSET => checkParam(Array(ZSET_NAME))
        case RedisSaveType.LIST => checkParam(Array(LIST_NAME))
        case _ => new CheckResult(false, "Unknown redis config. redis_save_type must be in [KV HASH SET ZSET LIST]")
      }
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {

    val conf = prepareEnv.getSparkSession.conf
    conf.set("spark.redis.host", config.getString("redis_host"))
    conf.set("spark.redis.port", config.getString("redis_port"))
    if (config.hasPath("redis.auth")) {
      conf.set("spark.redis.auth", "passwd")
    }
  }

  def dealWithKV(data: Dataset[Row])(implicit sc: SparkContext): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisKV(value)
  }

  def dealWithList(data: Dataset[Row], listName: String)(implicit sc: SparkContext): Unit = {
    val value = data.rdd.map(x => x.getString(0))
    sc.toRedisLIST(value, listName)
  }

  def dealWithSet(data: Dataset[Row], setName: String)(implicit sc: SparkContext): Unit = {
    val value = data.rdd.map(x => x.getString(0))
    sc.toRedisSET(value, setName)
  }

  def dealWithZSet(data: Dataset[Row], setName: String)(implicit sc: SparkContext): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisZSET(value, setName)
  }

  def dealWithHASH(data: Dataset[Row], hashName: String)(implicit sc: SparkContext): Unit = {
    val value = data.rdd.map(x => (x.getString(0), x.getString(1)))
    sc.toRedisHASH(value, hashName)
  }
}

object RedisSaveType extends Enumeration {
  def RedisSaveType: Value = Value

  val KV: Value = Value("KV")
  val HASH: Value = Value("HASH")
  val LIST: Value = Value("LIST")
  val SET: Value = Value("SET")
  val ZSET: Value = Value("ZSET")
}

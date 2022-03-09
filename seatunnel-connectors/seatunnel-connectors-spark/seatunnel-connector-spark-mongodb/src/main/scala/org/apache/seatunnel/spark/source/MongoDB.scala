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

import scala.collection.JavaConversions._

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.seatunnel.spark.utils.SparkStructTypeUtil
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType

class MongoDB extends SparkBatchSource {

  var readConfig: ReadConfig = _

  val confPrefix = "readconfig."

  var schema = new StructType()

  override def prepare(env: SparkEnvironment): Unit = {
    val map = new collection.mutable.HashMap[String, String]

    TypesafeConfigUtils
      .extractSubConfig(config, confPrefix, false)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        map.put(key, value)
      })

    if (config.hasPath("schema")) {
      val schemaJson = JSON.parseObject(config.getString("schema"))
      schema = SparkStructTypeUtil.getStructType(schema, schemaJson)
    }

    readConfig = ReadConfig(map)
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    if (schema.length > 0) {
      MongoSpark.builder().sparkSession(env.getSparkSession).readConfig(readConfig).build().toDF(
        schema)
    } else {
      MongoSpark.load(env.getSparkSession, readConfig)
    }
  }

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "readconfig.uri", "readconfig.database", "readconfig.collection")
  }

}

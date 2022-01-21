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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

class Doris extends SparkBatchSink with Serializable {

  var apiUrl: String = _
  var batch_size: Int = 100
  var column_separator: String = "\t"
  var propertiesMap = new mutable.HashMap[String, String]()

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val user: String = config.getString(Config.USER)
    val password: String = config.getString(Config.PASSWORD)
    if (propertiesMap.contains(Config.COLUMN_SEPARATOR)) {
      column_separator = propertiesMap(Config.COLUMN_SEPARATOR)
    }
    val sparkSession = env.getSparkSession
    import sparkSession.implicits._
    val fields = data.schema.fields
    val dataFrame = data.map(row => {
      val builder = new StringBuilder
      fields.foreach(f => {
        val filedValue = row.getAs[Any](f.name)
        builder.append(filedValue).append(column_separator)
      })
      builder.substring(0, builder.length - 1)
    })
    dataFrame.foreachPartition { partition =>
      var count: Int = 0
      val buffer = new ListBuffer[String]
      val dorisUtil = new DorisUtil(propertiesMap.toMap, apiUrl, user, password)
      for (message <- partition) {
        count += 1
        buffer += message
        if (count > batch_size) {
          dorisUtil.saveMessages(buffer.mkString("\n"))
          buffer.clear()
          count = 0
        }
      }
      dorisUtil.saveMessages(buffer.mkString("\n"))
    }
  }

  override def checkConfig(): CheckResult = {
    val checkResult =
      checkAllExists(config, Config.HOST, Config.DATABASE, Config.TABLE_NAME, Config.USER, Config.PASSWORD)

    if (!checkResult.isSuccess) {
      checkResult
    } else if (config.hasPath(Config.USER) && !config.hasPath(Config.PASSWORD) || config.hasPath(
        Config.PASSWORD) && !config.hasPath(Config.USER)) {
      CheckResult.error(Config.CHECK_USER_ERROR)
    } else {
      val host: String = config.getString(Config.HOST)
      val dataBase: String = config.getString(Config.DATABASE)
      val tableName: String = config.getString(Config.TABLE_NAME)
      this.apiUrl = s"http://$host/api/$dataBase/$tableName/_stream_load"
      if (TypesafeConfigUtils.hasSubConfig(config, Config.ARGS_PREFIX)) {
        val properties = TypesafeConfigUtils.extractSubConfig(config, Config.ARGS_PREFIX, true)
        val iterator = properties.entrySet().iterator()
        while (iterator.hasNext) {
          val map = iterator.next()
          val split = map.getKey.split("\\.")
          if (split.size == 2) {
            propertiesMap.put(split(1), String.valueOf(map.getValue.unwrapped))
          }
        }
      }
      CheckResult.success()
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    if (config.hasPath(Config.BULK_SIZE) && config.getInt(Config.BULK_SIZE) > 0) {
      batch_size = config.getInt(Config.BULK_SIZE)
    }
  }
}

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

package org.apache.seatunnel.spark.transform

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.transform.service.EtlHandler
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.seatunnel.spark.transform.EtlConfig._
import org.apache.seatunnel.spark.transform.service.impl.{DoNothingHandler, DefaultHandler}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable

class Etl extends BaseSparkTransform {

  private var handlerMap = mutable.Map[String, EtlHandler]()

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val etlType = config.getString(TYPE)

    handlerMap.getOrElse(etlType, DoNothingHandler()).handler(df, config)

  }


  override def checkConfig(): CheckResult = {
    checkAllExists(config, TYPE)
  }

  override def getPluginName: String = PLUGIN_NAME

  override def prepare(env: SparkEnvironment): Unit = {
    handlerMap += ("default" -> DefaultHandler())
  }

}

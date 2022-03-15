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

import java.util.concurrent.TimeUnit

import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

class Sleep extends BaseSparkTransform {

  private val logger: Logger = LoggerFactory.getLogger(classOf[Sleep])

  private[transform] val KEY_TIMEUNIT = "timeunit"

  private[transform] val KEY_TIMEOUT = "timeout"

  private[transform] val VALID_TIMEUNITS: Set[String] = TimeUnit.values().map(it => it.name()).toSet

  override def checkConfig(): CheckResult = {
    val result = CheckConfigUtil.checkAllExists(config, KEY_TIMEOUT)
    result.isSuccess match {
      case true =>
        config.hasPath(KEY_TIMEUNIT) match {
          case true =>
            val timeunit = config.getString(KEY_TIMEUNIT)
            VALID_TIMEUNITS.contains(timeunit) match {
              case true => result
              case false => CheckResult.error(s"TimeUnit '$timeunit' is invalid!")
            }
          case false => result
        }
      case false => result
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {}

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val timeunit = config.hasPath(KEY_TIMEUNIT) match {
      case true => TimeUnit.valueOf(config.getString(KEY_TIMEUNIT))
      case false => TimeUnit.SECONDS
    }

    val timeout = config.getLong(KEY_TIMEOUT)
    logger.info(s"Sleep start, will sleep $timeout $timeunit ...")
    timeunit.sleep(timeout)
    logger.info("Sleep end.")
    data
  }
}

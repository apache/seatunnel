/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.output.utils

import io.github.interestinglab.waterdrop.config.Config
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

object StructuredUtils {

  def setCheckpointLocation(dw: DataStreamWriter[Row], config: Config): DataStreamWriter[Row] = {
    if (config.hasPath("checkpointLocation")) {
      dw.option("checkpointLocation", config.getString("checkpointLocation"))
    } else {
      dw
    }
  }

  def checkTriggerMode(config: Config): Boolean = {
    config.hasPath("triggerMode") match {
      case true => {
        val triggerMode = config.getString("triggerMode")
        triggerMode match {
          case "ProcessingTime" | "Continuous" => {
            if (config.hasPath("interval")) {
              true
            } else {
              false
            }
          }
          case _ => true
        }
      }
      case false => true
    }
  }

  def writeWithTrigger(config: Config, writer: DataStreamWriter[Row]): DataStreamWriter[Row] = {
    config.getString("trigger_type") match {
      case "default" => writer
      case "ProcessingTime" => writer.trigger(Trigger.ProcessingTime(config.getString("interval")))
      case "OneTime" => writer.trigger(Trigger.Once())
      case "Continuous" => writer.trigger(Trigger.Continuous(config.getString("interval")))
    }
  }
}

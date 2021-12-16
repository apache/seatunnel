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
package io.github.interestinglab.waterdrop.spark.structuredstream

import java.util.{List => JList}

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.env.Execution
import io.github.interestinglab.waterdrop.spark.{BaseSparkTransform, SparkEnvironment}

class StructuredStreamingExecution(environment: SparkEnvironment) extends Execution[StructuredStreamingSource, BaseSparkTransform, StructuredStreamingSink] {

  private var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(void: Void): Unit = {}

  override def start(sources: JList[StructuredStreamingSource],
                     transforms: JList[BaseSparkTransform],
                     sinks: JList[StructuredStreamingSink]): Unit = {

  }
}

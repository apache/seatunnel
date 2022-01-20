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
package org.apache.seatunnel.spark.batch

import org.apache.seatunnel.common.config.{CheckResult, ConfigRuntimeException}
import org.apache.seatunnel.shade.com.typesafe.config.{Config, ConfigFactory}
import org.apache.seatunnel.env.Execution
import org.apache.seatunnel.spark.{BaseSparkSink, BaseSparkSource, BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}
import java.util.{List => JList}

import scala.collection.JavaConversions._

class SparkBatchExecution(environment: SparkEnvironment)
  extends Execution[SparkBatchSource, BaseSparkTransform, SparkBatchSink] {

  private var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = CheckResult.success()

  override def prepare(prepareEnv: Void): Unit = {}

  override def start(sources: JList[SparkBatchSource], transforms: JList[BaseSparkTransform], sinks: JList[SparkBatchSink]): Unit = {

    sources.foreach(s => {
      SparkBatchExecution.registerInputTempView(
        s.asInstanceOf[BaseSparkSource[Dataset[Row]]],
        environment)
    })
    if (!sources.isEmpty) {
      var ds = sources.get(0).getData(environment)
      for (tf <- transforms) {

        if (ds.take(1).length > 0) {
          ds = SparkBatchExecution.transformProcess(environment, tf, ds)
          SparkBatchExecution.registerTransformTempView(tf, ds)
        }
      }

      // if (ds.take(1).length > 0) {
      sinks.foreach(sink => {
        SparkBatchExecution.sinkProcess(environment, sink, ds)
      })
      // }
    }
  }

}

object SparkBatchExecution {

  private[seatunnel] val sourceTableName = "source_table_name"
  private[seatunnel] val resultTableName = "result_table_name"

  private[seatunnel] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
  }

  private[seatunnel] def registerInputTempView(source: BaseSparkSource[Dataset[Row]], environment: SparkEnvironment): Unit = {
    val conf = source.getConfig
    conf.hasPath(SparkBatchExecution.resultTableName) match {
      case true =>
        val tableName = conf.getString(SparkBatchExecution.resultTableName)
        registerTempView(tableName, source.getData(environment))
      case false =>
        throw new ConfigRuntimeException(
          "Plugin[" + source.getClass.getName + "] must be registered as dataset/table, please set \"result_table_name\" config")
    }
  }

  private[seatunnel] def transformProcess(environment: SparkEnvironment, transform: BaseSparkTransform, ds: Dataset[Row]): Dataset[Row] = {
    val config = transform.getConfig()
    val fromDs = config.hasPath(SparkBatchExecution.sourceTableName) match {
      case true =>
        val sourceTableName = config.getString(SparkBatchExecution.sourceTableName)
        environment.getSparkSession.read.table(sourceTableName)
      case false => ds
    }

    transform.process(fromDs, environment)
  }

  private[seatunnel] def registerTransformTempView(plugin: BaseSparkTransform, ds: Dataset[Row]): Unit = {
    val config = plugin.getConfig()
    if (config.hasPath(SparkBatchExecution.resultTableName)) {
      val tableName = config.getString(SparkBatchExecution.resultTableName)
      registerTempView(tableName, ds)
    }
  }

  private[seatunnel] def sinkProcess(environment: SparkEnvironment, sink: BaseSparkSink[_], ds: Dataset[Row]): Unit = {
    val config = sink.getConfig()
    val fromDs = config.hasPath(SparkBatchExecution.sourceTableName) match {
      case true =>
        val sourceTableName = config.getString(SparkBatchExecution.sourceTableName)
        environment.getSparkSession.read.table(sourceTableName)
      case false => ds
    }

    sink.output(fromDs, environment)
  }
}

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
package org.apache.seatunnel.spark.stream

import org.apache.seatunnel.apis.base.env.Execution
import org.apache.seatunnel.apis.base.plugin.Plugin
import org.apache.seatunnel.shade.com.typesafe.config.{Config, ConfigFactory}
import org.apache.seatunnel.spark.{BaseSparkSink, BaseSparkSource, BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

import java.util.{List => JList}
import scala.collection.JavaConversions._

class SparkStreamingExecution(sparkEnvironment: SparkEnvironment)
  extends Execution[BaseSparkSource[_], BaseSparkTransform, BaseSparkSink[_], SparkEnvironment] {

  private var config = ConfigFactory.empty()

  override def start(sources: JList[BaseSparkSource[_]], transforms: JList[BaseSparkTransform], sinks: JList[BaseSparkSink[_]]): Unit = {
    val source = sources.get(0).asInstanceOf[SparkStreamingSource[_]]

    sources.subList(1, sources.size()).foreach(s => {
      SparkEnvironment.registerInputTempView(
        s.asInstanceOf[BaseSparkSource[Dataset[Row]]],
        sparkEnvironment)
    })
    source.start(
      sparkEnvironment,
      dataset => {
        val conf = source.getConfig
        if (conf.hasPath(Plugin.RESULT_TABLE_NAME)) {
          SparkEnvironment.registerTempView(
            conf.getString(Plugin.RESULT_TABLE_NAME),
            dataset)
        }
        var ds = dataset

        if (ds.take(1).length > 0) {
          for (tf <- transforms) {
            ds = SparkEnvironment.transformProcess(sparkEnvironment, tf, ds)
            SparkEnvironment.registerTransformTempView(tf, ds)
          }
        }

        source.beforeOutput()

        if (ds.take(1).length > 0) {
          sinks.foreach(sink => {
            SparkEnvironment.sinkProcess(sparkEnvironment, sink, ds)
          })
        }

        source.afterOutput()
      })

    val streamingContext = sparkEnvironment.getStreamingContext
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

}

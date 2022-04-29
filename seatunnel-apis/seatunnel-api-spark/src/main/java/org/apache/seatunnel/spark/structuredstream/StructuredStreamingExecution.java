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

package org.apache.seatunnel.spark.structuredstream;

import org.apache.seatunnel.apis.base.env.Execution;
import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.stream.Collectors;

public class StructuredStreamingExecution implements Execution<StructuredStreamingSource, BaseSparkTransform, StructuredStreamingSink, SparkEnvironment> {

    private final SparkEnvironment sparkEnvironment;

    private Config config = ConfigFactory.empty();

    public StructuredStreamingExecution(SparkEnvironment sparkEnvironment) {
        this.sparkEnvironment = sparkEnvironment;
    }

    @Override
    public void start(List<StructuredStreamingSource> sources, List<BaseSparkTransform> transforms,
        List<StructuredStreamingSink> sinks) throws Exception {

        List<Dataset<Row>> datasetList = sources.stream().map(s ->
                SparkEnvironment.registerInputTempView(s, sparkEnvironment)
        ).collect(Collectors.toList());
        if (datasetList.size() > 0) {
            Dataset<Row> ds = datasetList.get(0);
            for (BaseSparkTransform tf : transforms) {
                ds = SparkEnvironment.transformProcess(sparkEnvironment, tf, ds);
                SparkEnvironment.registerTransformTempView(tf, ds);
            }

            for (StructuredStreamingSink sink : sinks) {
                SparkEnvironment.sinkProcess(sparkEnvironment, sink, ds).start();
            }
            sparkEnvironment.getSparkSession().streams().awaitAnyTermination();
        }
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

}

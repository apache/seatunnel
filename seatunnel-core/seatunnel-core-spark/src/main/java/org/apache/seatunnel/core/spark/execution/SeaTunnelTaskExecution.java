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

package org.apache.seatunnel.core.spark.execution;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.config.EnvironmentFactory;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SeaTunnelTaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeaTunnelTaskExecution.class);

    private final Config config;
    private final SparkEnvironment sparkEnvironment;
    private final PluginExecuteProcessor sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor transformPluginExecuteProcessor;
    private final PluginExecuteProcessor sinkPluginExecuteProcessor;

    public SeaTunnelTaskExecution(Config config) {
        this.config = config;
        this.sparkEnvironment = (SparkEnvironment) new EnvironmentFactory<>(config, EngineType.SPARK).getEnvironment();
        SeaTunnelContext.getContext().setJobMode(sparkEnvironment.getJobMode());
        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(sparkEnvironment, config.getConfigList("source"));
        this.transformPluginExecuteProcessor = new TransformExecuteProcessor(sparkEnvironment, config.getConfigList("transform"));
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(sparkEnvironment, config.getConfigList("sink"));
    }

    public void execute() throws Exception {
        List<Dataset<Row>> datasets = new ArrayList<>();
        datasets = sourcePluginExecuteProcessor.execute(datasets);
        datasets = transformPluginExecuteProcessor.execute(datasets);
        sinkPluginExecuteProcessor.execute(datasets);

        LOGGER.info("Spark Execution started");
    }
}

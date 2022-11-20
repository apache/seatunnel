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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.spark.config.SparkEnvironmentFactory;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class SparkExecution {

    private final SparkEnvironment sparkEnvironment;
    private final PluginExecuteProcessor sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor transformPluginExecuteProcessor;
    private final PluginExecuteProcessor sinkPluginExecuteProcessor;

    public SparkExecution(Config config) {
        this.sparkEnvironment = new SparkEnvironmentFactory(config).getEnvironment();
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(sparkEnvironment.getJobMode());
        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(sparkEnvironment, jobContext, config.getConfigList(Constants.SOURCE));
        this.transformPluginExecuteProcessor = new TransformExecuteProcessor(sparkEnvironment, jobContext,
            TypesafeConfigUtils.getConfigList(config, Constants.TRANSFORM, Collections.emptyList()));
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(sparkEnvironment, jobContext, config.getConfigList(Constants.SINK));
    }

    public void execute() throws TaskExecuteException {
        List<Dataset<Row>> datasets = new ArrayList<>();
        datasets = sourcePluginExecuteProcessor.execute(datasets);
        datasets = transformPluginExecuteProcessor.execute(datasets);
        sinkPluginExecuteProcessor.execute(datasets);

        log.info("Spark Execution started");
    }
}

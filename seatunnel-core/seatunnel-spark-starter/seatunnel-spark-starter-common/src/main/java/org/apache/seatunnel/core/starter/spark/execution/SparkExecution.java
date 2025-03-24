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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class SparkExecution implements TaskExecution {
    private final SparkRuntimeEnvironment sparkRuntimeEnvironment;
    private final PluginExecuteProcessor<DatasetTableInfo, SparkRuntimeEnvironment>
            sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor<DatasetTableInfo, SparkRuntimeEnvironment>
            transformPluginExecuteProcessor;
    private final PluginExecuteProcessor<DatasetTableInfo, SparkRuntimeEnvironment>
            sinkPluginExecuteProcessor;

    public SparkExecution(Config config) {
        this.sparkRuntimeEnvironment = SparkRuntimeEnvironment.getInstance(config);
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));
        jobContext.setEnableCheckpoint(RuntimeEnvironment.getEnableCheckpoint(config));

        this.sourcePluginExecuteProcessor =
                new SourceExecuteProcessor(
                        sparkRuntimeEnvironment,
                        jobContext,
                        config.getConfigList(Constants.SOURCE));
        this.transformPluginExecuteProcessor =
                new TransformExecuteProcessor(
                        sparkRuntimeEnvironment,
                        jobContext,
                        TypesafeConfigUtils.getConfigList(
                                config, Constants.TRANSFORM, Collections.emptyList()));
        this.sinkPluginExecuteProcessor =
                new SinkExecuteProcessor(
                        sparkRuntimeEnvironment, jobContext, config.getConfigList(Constants.SINK));
    }

    @Override
    public void execute() throws TaskExecuteException {
        List<DatasetTableInfo> datasets = new ArrayList<>();
        datasets = sourcePluginExecuteProcessor.execute(datasets);
        datasets = transformPluginExecuteProcessor.execute(datasets);
        sinkPluginExecuteProcessor.execute(datasets);
        log.info("Spark Execution started");
    }

    public SparkRuntimeEnvironment getSparkRuntimeEnvironment() {
        return sparkRuntimeEnvironment;
    }
}

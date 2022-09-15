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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.core.starter.flink.config.FlinkEnvironmentFactory;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to execute a SeaTunnelTask.
 */
public class FlinkExecution implements TaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);
    private final FlinkEnvironment flinkEnvironment;
    private final PluginExecuteProcessor sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor transformPluginExecuteProcessor;
    private final PluginExecuteProcessor sinkPluginExecuteProcessor;

    public FlinkExecution(Config config) {
        this.flinkEnvironment = new FlinkEnvironmentFactory(config).getEnvironment();
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(flinkEnvironment.getJobMode());
        registerPlugin();
        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(flinkEnvironment, jobContext, config.getConfigList(Constants.SOURCE));
        this.transformPluginExecuteProcessor = new TransformExecuteProcessor(flinkEnvironment, jobContext, config.getConfigList(Constants.TRANSFORM));
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(flinkEnvironment, jobContext, config.getConfigList(Constants.SINK));
    }

    @Override
    public void execute() throws TaskExecuteException {
        List<DataStream<Row>> dataStreams = new ArrayList<>();
        dataStreams = sourcePluginExecuteProcessor.execute(dataStreams);
        dataStreams = transformPluginExecuteProcessor.execute(dataStreams);
        sinkPluginExecuteProcessor.execute(dataStreams);

        LOGGER.info("Flink Execution Plan:{}", flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
        try {
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            throw new TaskExecuteException("Execute Flink job error", e);
        }
    }

    private void registerPlugin() {
        List<URL> pluginsJarDependencies = Common.getPluginsJarDependencies().stream()
            .map(Path::toUri)
            .map(uri -> {
                try {
                    return uri.toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException("the uri of jar illegal:" + uri, e);
                }
            })
            .collect(Collectors.toList());

        pluginsJarDependencies.forEach(url -> Common.ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url));

        flinkEnvironment.registerPlugin(pluginsJarDependencies);
    }
}

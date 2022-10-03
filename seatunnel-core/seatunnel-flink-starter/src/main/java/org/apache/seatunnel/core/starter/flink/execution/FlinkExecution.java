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
import org.apache.seatunnel.core.starter.flink.config.FlinkCommon;
import org.apache.seatunnel.core.starter.flink.config.FlinkEnvironmentFactory;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final List<URL> jarPaths;

    public FlinkExecution(Config config) {
        jarPaths = new ArrayList<>();
        registerPlugin();
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(new FlinkEnvironmentFactory(config).getJobMode(config.getConfig("env")));

        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(jarPaths, config.getConfigList(Constants.SOURCE), jobContext);
        this.transformPluginExecuteProcessor = new TransformExecuteProcessor(jarPaths, config.getConfigList(Constants.TRANSFORM), jobContext);
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(jarPaths, config.getConfigList(Constants.SINK), jobContext);

        this.flinkEnvironment = new FlinkEnvironmentFactory(this.registerPlugin(config, jarPaths)).getEnvironment();

        this.sourcePluginExecuteProcessor.setFlinkEnvironment(flinkEnvironment);
        this.transformPluginExecuteProcessor.setFlinkEnvironment(flinkEnvironment);
        this.sinkPluginExecuteProcessor.setFlinkEnvironment(flinkEnvironment);

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

        pluginsJarDependencies.forEach(url -> FlinkCommon.ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url));

        jarPaths.addAll(pluginsJarDependencies);
    }

    private Config registerPlugin(Config config, List<URL> jars) {
        config = this.parseConfig(config, "env." + PipelineOptions.JARS.key(), jars);
        return this.parseConfig(config, "env." + PipelineOptions.CLASSPATHS.key(), jars);
    }

    private Config parseConfig(Config config, String path, List<URL> jars) {

        if (config.hasPath(path)) {
            List<URL> paths = Arrays.stream(config.getString(path).split(";")).map(s -> {
                try {
                    return new URL(s);
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                return null;
            }).collect(Collectors.toList());
            paths.addAll(jars);

            config = config.withValue(path,
                ConfigValueFactory.fromAnyRef(paths.stream().map(URL::toString).collect(Collectors.joining(";"))));

        } else {
            config = config.withValue(path,
                ConfigValueFactory.fromAnyRef(jars.stream().map(URL::toString).collect(Collectors.joining(";"))));
        }
        return config;
    }
}

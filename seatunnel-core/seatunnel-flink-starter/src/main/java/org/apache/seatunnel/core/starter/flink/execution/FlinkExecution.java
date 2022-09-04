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

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.starter.config.EngineType;
import org.apache.seatunnel.core.starter.config.EnvironmentFactory;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to execute a SeaTunnelTask.
 */
public class FlinkExecution implements TaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);
    private static final int PLUGIN_LIB_DIR_DEPTH = 3;

    private final Config config;
    private final FlinkEnvironment flinkEnvironment;
    private final PluginExecuteProcessor sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor transformPluginExecuteProcessor;
    private final PluginExecuteProcessor sinkPluginExecuteProcessor;

    public FlinkExecution(Config config) {
        this.config = config;
        this.flinkEnvironment = new EnvironmentFactory<FlinkEnvironment>(config, EngineType.FLINK).getEnvironment();
        SeaTunnelContext.getContext().setJobMode(flinkEnvironment.getJobMode());
        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(flinkEnvironment, config.getConfigList("source"));
        this.transformPluginExecuteProcessor = new TransformExecuteProcessor(flinkEnvironment, config.getConfigList("transform"));
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(flinkEnvironment, config.getConfigList("sink"));
        List<URL> pluginsJarDependencies = getPluginsJarDependencies();
        flinkEnvironment.registerPlugin(pluginsJarDependencies);
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

    /**
     * return plugin's dependent jars, which located in 'plugins/${pluginName}/lib/*'.
     */
    private List<URL> getPluginsJarDependencies() {
        Path pluginRootDir = Common.pluginRootDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(pluginRootDir, PLUGIN_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream
                .filter(it -> pluginRootDir.relativize(it).getNameCount() == PLUGIN_LIB_DIR_DEPTH)
                .filter(it -> it.getParent().endsWith("lib"))
                .filter(it -> it.getFileName().toString().endsWith(".jar"))
                .map(Path::toUri)
                .map(pluginJar -> {
                    try {
                        return pluginJar.toURL();
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("the plugin jar URL is illegal: " + pluginJar, e);
                    }
                })
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

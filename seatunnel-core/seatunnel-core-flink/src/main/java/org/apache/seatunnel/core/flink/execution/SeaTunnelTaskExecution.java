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

package org.apache.seatunnel.core.flink.execution;

import static org.apache.seatunnel.apis.base.plugin.Plugin.RESULT_TABLE_NAME;
import static org.apache.seatunnel.apis.base.plugin.Plugin.SOURCE_TABLE_NAME;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.config.EnvironmentFactory;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFlinkTransformPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.flink.sink.FlinkSinkConverter;
import org.apache.seatunnel.translation.flink.source.SeaTunnelParallelSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Used to execute a SeaTunnelTask.
 */
public class SeaTunnelTaskExecution {

    private final Config config;
    private final FlinkEnvironment flinkEnvironment;

    // todo: initialize in sub class
    private final List<SeaTunnelParallelSource> sources;
    private final List<? extends Config> sourceConfigs;
    private final List<FlinkStreamTransform> transforms;
    private final List<? extends Config> transformConfigs;
    private final List<Sink> sinks;
    private final List<? extends Config> sinkConfigs;
    private final List<URL> pluginJars = new ArrayList<>();

    public SeaTunnelTaskExecution(Config config) {
        this.config = config;
        // todo: create the environment
        this.flinkEnvironment = (FlinkEnvironment) new EnvironmentFactory<>(config, EngineType.FLINK).getEnvironment();
        this.flinkEnvironment.setJobMode(JobMode.STREAMING);
        this.flinkEnvironment.prepare();
        this.sourceConfigs = config.getConfigList("source");
        this.transformConfigs = config.getConfigList("transform");
        this.sinkConfigs = config.getConfigList("sink");

        this.sources = initializeSources(sourceConfigs);
        this.transforms = initializeTransforms(transformConfigs);
        this.sinks = initializeSinks(sinkConfigs);
        flinkEnvironment.registerPlugin(pluginJars);
    }

    public void execute() throws Exception {
        List<DataStream<Row>> data = new ArrayList<>();
        StreamExecutionEnvironment executionEnvironment = flinkEnvironment.getStreamExecutionEnvironment();
        for (int i = 0; i < sources.size(); i++) {
            DataStreamSource<Row> sourceStream = executionEnvironment.addSource(sources.get(i));
            data.add(sourceStream);
            Config pluginConfig = sourceConfigs.get(i);
            registerResultTable(pluginConfig, sourceStream);
        }

        DataStream<Row> input = data.get(0);

        for (int i = 0; i < transforms.size(); i++) {
            FlinkStreamTransform transform = transforms.get(i);
            transform.setConfig(transformConfigs.get(i));
            transform.prepare(flinkEnvironment);
            DataStream<Row> stream = fromSourceTable(transformConfigs.get(i)).orElse(input);
            input = transform.processStream(flinkEnvironment, stream);
            Config pluginConfig = transformConfigs.get(i);
            registerResultTable(pluginConfig, input);
            transform.registerFunction(flinkEnvironment);
        }

        for (int i = 0; i < sinks.size(); i++) {
            Config sinkConfig = sinkConfigs.get(i);
            DataStream<Row> stream = fromSourceTable(sinkConfig).orElse(input);
            stream.sinkTo(sinks.get(i));
        }
        executionEnvironment.execute();
    }

    private List<Sink> initializeSinks(List<? extends Config> sinkConfigs) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        FlinkSinkConverter<SeaTunnelRow, Row, Object, Object, Object> flinkSinkConverter = new FlinkSinkConverter<>();
        return sinkConfigs.stream()
            .map(sinkConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                    "seatunnel",
                    "sink",
                    sinkConfig.getString("plugin_name"));
                pluginJars.addAll(sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
                SeaTunnelSink<SeaTunnelRow, Object, Object, Object> pluginInstance = sinkPluginDiscovery.getPluginInstance(pluginIdentifier);
                return flinkSinkConverter.convert(pluginInstance, Collections.emptyMap());
            }).collect(Collectors.toList());
    }

    private List<FlinkStreamTransform> initializeTransforms(List<? extends Config> transformConfigs) {
        SeaTunnelFlinkTransformPluginDiscovery transformPluginDiscovery = new SeaTunnelFlinkTransformPluginDiscovery();
        return transformConfigs.stream()
            .map(transformConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                    "seatunnel",
                    "transform",
                    transformConfig.getString("plugin_name"));
                pluginJars.addAll(transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
                return (FlinkStreamTransform) transformPluginDiscovery.getPluginInstance(pluginIdentifier);
            }).collect(Collectors.toList());
    }

    private List<SeaTunnelParallelSource> initializeSources(List<? extends Config> sourceConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        List<SeaTunnelParallelSource> seaTunnelSources = new ArrayList<>();
        for (int i = 0; i < sourceConfigs.size(); i++) {
            Config sourceConfig = sourceConfigs.get(i);
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                "seatunnel",
                "source",
                sourceConfig.getString("plugin_name"));
            pluginJars.addAll(sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            seaTunnelSources.add(new SeaTunnelParallelSource(sourcePluginDiscovery.getPluginInstance(pluginIdentifier)));
        }
        return seaTunnelSources;
    }

    private void registerResultTable(Config pluginConfig, DataStream<Row> dataStream) {
        if (pluginConfig.hasPath(RESULT_TABLE_NAME)) {
            String name = pluginConfig.getString(RESULT_TABLE_NAME);
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (pluginConfig.hasPath("field_name")) {
                    String fieldName = pluginConfig.getString("field_name");
                    tableEnvironment.registerDataStream(name, dataStream, fieldName);
                } else {
                    tableEnvironment.registerDataStream(name, dataStream);
                }
            }
        }
    }

    private Optional<DataStream<Row>> fromSourceTable(Config pluginConfig) {
        if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(pluginConfig.getString(SOURCE_TABLE_NAME));
            return Optional.ofNullable(TableUtil.tableToDataStream(tableEnvironment, table, true));
        }
        return Optional.empty();
    }

    public String getExecutionPlan() {
        return flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan();
    }
}

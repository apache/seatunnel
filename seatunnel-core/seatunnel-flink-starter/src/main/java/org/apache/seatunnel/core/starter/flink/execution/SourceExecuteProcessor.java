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

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.flink.source.SeaTunnelParallelSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class SourceExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelParallelSource> {

    private static final String PLUGIN_TYPE = "source";

    public SourceExecuteProcessor(FlinkEnvironment flinkEnvironment,
                                  List<? extends Config> sourceConfigs) {
        super(flinkEnvironment, sourceConfigs);
    }

    @Override
    public List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams) {
        StreamExecutionEnvironment executionEnvironment = flinkEnvironment.getStreamExecutionEnvironment();
        List<DataStream<Row>> sources = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            DataStreamSource<Row> sourceStream = executionEnvironment.addSource(plugins.get(i));
            Config pluginConfig = pluginConfigs.get(i);
            registerResultTable(pluginConfig, sourceStream);
            sources.add(sourceStream);
        }
        return sources;
    }

    @Override
    protected List<SeaTunnelParallelSource> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        List<SeaTunnelParallelSource> sources = new ArrayList<>();
        List<URL> jars = new ArrayList<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                ENGINE_TYPE, PLUGIN_TYPE, sourceConfig.getString(PLUGIN_NAME));
            jars.addAll(sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SeaTunnelSource seaTunnelSource = sourcePluginDiscovery.getPluginInstance(pluginIdentifier);
            seaTunnelSource.prepare(sourceConfig);
            seaTunnelSource.setSeaTunnelContext(SeaTunnelContext.getContext());
            sources.add(new SeaTunnelParallelSource(seaTunnelSource));
        }
        flinkEnvironment.registerPlugin(jars);
        return sources;
    }
}

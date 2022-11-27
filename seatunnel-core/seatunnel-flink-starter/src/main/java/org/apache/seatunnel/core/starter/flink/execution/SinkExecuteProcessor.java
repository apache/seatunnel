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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SourceCommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;
import org.apache.seatunnel.translation.flink.utils.TypeConverterUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.Serializable;

public class SinkExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> {

    private static final String PLUGIN_TYPE = "sink";

    protected SinkExecuteProcessor(List<URL> jarPaths, List<? extends Config> pluginConfigs, JobContext jobContext) {
        super(jarPaths, pluginConfigs, jobContext);
    }

    @Override
    protected List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> initializePlugins(List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery(addUrlToClassloader);
        List<URL> pluginJars = new ArrayList<>();
        List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> sinks = pluginConfigs.stream().map(sinkConfig -> {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, sinkConfig.getString(PLUGIN_NAME));
            pluginJars.addAll(sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink =
                sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
            seaTunnelSink.prepare(sinkConfig);
            seaTunnelSink.setJobContext(jobContext);
            return seaTunnelSink;
        }).distinct().collect(Collectors.toList());
        jarPaths.addAll(pluginJars);
        return sinks;
    }

    @Override
    public List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams) throws TaskExecuteException {
        DataStream<Row> input = upstreamDataStreams.get(0);
        for (int i = 0; i < plugins.size(); i++) {
            Config sinkConfig = pluginConfigs.get(i);
            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink = plugins.get(i);
            DataStream<Row> stream = fromSourceTable(sinkConfig).orElse(input);
            seaTunnelSink.setTypeInfo((SeaTunnelRowType) TypeConverterUtils.convert(stream.getType()));
            DataStreamSink<Row> dataStreamSink = stream.sinkTo(new FlinkSink<>(seaTunnelSink)).name(seaTunnelSink.getPluginName());
            if (sinkConfig.hasPath(SourceCommonOptions.PARALLELISM.key())) {
                int parallelism = sinkConfig.getInt(SourceCommonOptions.PARALLELISM.key());
                dataStreamSink.setParallelism(parallelism);
            }
        }
        // the sink is the last stream
        return null;
    }
}

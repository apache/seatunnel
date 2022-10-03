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
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFlinkTransformPluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransformExecuteProcessor extends AbstractPluginExecuteProcessor<FlinkStreamTransform> {

    private static final String PLUGIN_TYPE = "transform";

    protected TransformExecuteProcessor(List<URL> jarPaths, List<? extends Config> pluginConfigs, JobContext jobContext) {
        super(jarPaths, pluginConfigs, jobContext);
    }

    @Override
    protected List<FlinkStreamTransform> initializePlugins(List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelFlinkTransformPluginDiscovery transformPluginDiscovery = new SeaTunnelFlinkTransformPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        List<FlinkStreamTransform> transforms = pluginConfigs.stream()
            .map(transformConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, transformConfig.getString(PLUGIN_NAME));
                pluginJars.addAll(transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
                FlinkStreamTransform pluginInstance = (FlinkStreamTransform) transformPluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(transformConfig);
                pluginInstance.prepare(flinkEnvironment);
                return pluginInstance;
            }).distinct().collect(Collectors.toList());
        jarPaths.addAll(pluginJars);
        return transforms;
    }

    @Override
    public List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams) throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        DataStream<Row> input = upstreamDataStreams.get(0);
        List<DataStream<Row>> result = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            try {
                FlinkStreamTransform transform = plugins.get(i);
                Config pluginConfig = pluginConfigs.get(i);
                DataStream<Row> stream = fromSourceTable(pluginConfig).orElse(input);
                input = transform.processStream(flinkEnvironment, stream);
                registerResultTable(pluginConfig, input);
                transform.registerFunction(flinkEnvironment);
                result.add(input);
            } catch (Exception e) {
                throw new TaskExecuteException(
                    String.format("SeaTunnel transform task: %s execute error", plugins.get(i).getPluginName()), e);
            }
        }
        return result;
    }
}

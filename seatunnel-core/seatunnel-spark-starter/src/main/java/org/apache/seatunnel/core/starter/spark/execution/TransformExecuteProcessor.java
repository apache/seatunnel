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
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSparkTransformPluginDiscovery;
import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransformExecuteProcessor extends AbstractPluginExecuteProcessor<BaseSparkTransform> {

    private static final String PLUGIN_TYPE = "transform";

    protected TransformExecuteProcessor(SparkEnvironment sparkEnvironment,
                                        JobContext jobContext,
                                        List<? extends Config> pluginConfigs) {
        super(sparkEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<BaseSparkTransform> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSparkTransformPluginDiscovery transformPluginDiscovery = new SeaTunnelSparkTransformPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        List<BaseSparkTransform> transforms = pluginConfigs.stream()
            .map(transformConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, transformConfig.getString(PLUGIN_NAME));
                pluginJars.addAll(transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
                BaseSparkTransform pluginInstance = transformPluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(transformConfig);
                pluginInstance.prepare(sparkEnvironment);
                return pluginInstance;
            }).distinct().collect(Collectors.toList());
        sparkEnvironment.registerPlugin(pluginJars);
        return transforms;
    }

    @Override
    public List<Dataset<Row>> execute(List<Dataset<Row>> upstreamDataStreams) throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        Dataset<Row> input = upstreamDataStreams.get(0);
        List<Dataset<Row>> result = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            BaseSparkTransform transform = plugins.get(i);
            Config pluginConfig = pluginConfigs.get(i);
            Dataset<Row> stream = fromSourceTable(pluginConfig, sparkEnvironment).orElse(input);
            input = transform.process(stream, sparkEnvironment);
            registerInputTempView(pluginConfig, input);
            result.add(input);
        }
        return result;
    }
}

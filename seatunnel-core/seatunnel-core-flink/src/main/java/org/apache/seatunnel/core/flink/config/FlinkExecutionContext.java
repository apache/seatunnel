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

package org.apache.seatunnel.core.flink.config;

import org.apache.seatunnel.apis.base.api.BaseSink;
import org.apache.seatunnel.apis.base.api.BaseSource;
import org.apache.seatunnel.apis.base.api.BaseTransform;
import org.apache.seatunnel.core.base.config.AbstractExecutionContext;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.config.PluginType;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.flink.FlinkSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.flink.FlinkSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.flink.FlinkTransformPluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkExecutionContext extends AbstractExecutionContext<FlinkEnvironment> {
    private final FlinkSourcePluginDiscovery flinkSourcePluginDiscovery;
    private final FlinkTransformPluginDiscovery flinkTransformPluginDiscovery;
    private final FlinkSinkPluginDiscovery flinkSinkPluginDiscovery;
    private final List<URL> pluginJars;

    public FlinkExecutionContext(Config config, EngineType engine) {
        super(config, engine);
        this.flinkSourcePluginDiscovery = new FlinkSourcePluginDiscovery();
        this.flinkTransformPluginDiscovery = new FlinkTransformPluginDiscovery();
        this.flinkSinkPluginDiscovery = new FlinkSinkPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        pluginJars.addAll(flinkSourcePluginDiscovery.getPluginJarPaths(getPluginIdentifiers(PluginType.SOURCE)));
        pluginJars.addAll(flinkSinkPluginDiscovery.getPluginJarPaths(getPluginIdentifiers(PluginType.TRANSFORM)));
        this.pluginJars = pluginJars;
        this.getEnvironment().registerPlugin(pluginJars);
    }

    @Override
    public List<BaseSource<FlinkEnvironment>> getSources() {
        final String pluginType = PluginType.SOURCE.getType();
        final String engineType = EngineType.FLINK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseSource<FlinkEnvironment> pluginInstance = flinkSourcePluginDiscovery.getPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<BaseTransform<FlinkEnvironment>> getTransforms() {
        final String pluginType = PluginType.TRANSFORM.getType();
        final String engineType = EngineType.FLINK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseTransform<FlinkEnvironment> pluginInstance = flinkTransformPluginDiscovery.getPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<BaseSink<FlinkEnvironment>> getSinks() {
        final String pluginType = PluginType.SINK.getType();
        final String engineType = EngineType.FLINK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseSink<FlinkEnvironment> pluginInstance = flinkSinkPluginDiscovery.getPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<URL> getPluginJars() {
        return pluginJars;
    }
}

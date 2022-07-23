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

package org.apache.seatunnel.core.spark.config;

import org.apache.seatunnel.apis.base.api.BaseSink;
import org.apache.seatunnel.apis.base.api.BaseSource;
import org.apache.seatunnel.apis.base.api.BaseTransform;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.base.config.AbstractExecutionContext;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.spark.SparkSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.spark.SparkSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.spark.SparkTransformPluginDiscovery;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkExecutionContext extends AbstractExecutionContext<SparkEnvironment> {
    private final SparkSourcePluginDiscovery sparkSourcePluginDiscovery;
    private final SparkTransformPluginDiscovery sparkTransformPluginDiscovery;
    private final SparkSinkPluginDiscovery sparkSinkPluginDiscovery;
    private final List<URL> pluginJars;

    public SparkExecutionContext(Config config, EngineType engine) {
        super(config, engine);
        this.sparkSourcePluginDiscovery = new SparkSourcePluginDiscovery();
        this.sparkTransformPluginDiscovery = new SparkTransformPluginDiscovery();
        this.sparkSinkPluginDiscovery = new SparkSinkPluginDiscovery();
        Set<URL> pluginJars = new HashSet<>();
        pluginJars.addAll(sparkSourcePluginDiscovery.getPluginJarPaths(getPluginIdentifiers(PluginType.SOURCE)));
        pluginJars.addAll(sparkSinkPluginDiscovery.getPluginJarPaths(getPluginIdentifiers(PluginType.SINK)));
        this.pluginJars = new ArrayList<>(pluginJars);
        this.getEnvironment().registerPlugin(this.pluginJars);
    }

    @Override
    public List<BaseSource<SparkEnvironment>> getSources() {
        final String pluginType = PluginType.SOURCE.getType();
        final String engineType = EngineType.SPARK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseSource<SparkEnvironment> pluginInstance = sparkSourcePluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<BaseTransform<SparkEnvironment>> getTransforms() {
        final String pluginType = PluginType.TRANSFORM.getType();
        final String engineType = EngineType.SPARK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseTransform<SparkEnvironment> pluginInstance = sparkTransformPluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<BaseSink<SparkEnvironment>> getSinks() {
        final String pluginType = PluginType.SINK.getType();
        final String engineType = EngineType.SPARK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseSink<SparkEnvironment> pluginInstance = sparkSinkPluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<URL> getPluginJars() {
        return pluginJars;
    }
}

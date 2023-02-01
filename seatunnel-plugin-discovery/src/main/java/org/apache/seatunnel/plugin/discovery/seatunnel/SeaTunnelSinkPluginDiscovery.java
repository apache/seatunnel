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

package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class SeaTunnelSinkPluginDiscovery extends AbstractPluginDiscovery<SeaTunnelSink> {

    private static final String PLUGIN_TYPE = PluginType.SINK.getType();

    public SeaTunnelSinkPluginDiscovery() {
        super("seatunnel");
    }

    public SeaTunnelSinkPluginDiscovery(BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super("seatunnel", addURLToClassLoader);
    }

    @Override
    protected Class<SeaTunnelSink> getPluginBaseClass() {
        return SeaTunnelSink.class;
    }

    public ImmutablePair<List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>>, Set<URL>> initializePlugins(
        List<URL> jarForCreatePluginInstance, List<? extends Config> pluginConfigs, JobContext jobContext) {
        Set<URL> pluginJars = new HashSet<>();
        List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> sinks =
            pluginConfigs.stream().map(sinkConfig -> {
                PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, sinkConfig.getString(PLUGIN_NAME));
                pluginJars.addAll(getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
                SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink = null;
                if (CollectionUtils.isEmpty(jarForCreatePluginInstance)) {
                    seaTunnelSink = createPluginInstance(pluginIdentifier);
                } else {
                    seaTunnelSink = createPluginInstance(pluginIdentifier, jarForCreatePluginInstance);
                }

                // check save mode config
                if (SupportDataSaveMode.class.isAssignableFrom(seaTunnelSink.getClass())) {
                    SupportDataSaveMode saveModeSink = (SupportDataSaveMode) seaTunnelSink;
                    List<TableSinkFactory> factories =
                        FactoryUtil.discoverFactories(new URLClassLoader(pluginJars.toArray(new URL[0])),
                            TableSinkFactory.class);
                    if (CollectionUtils.isEmpty(factories)) {
                        throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.PLUGIN_INITIALIZE_FAILED,
                            String.format("Sink connector [%s] need implement TableSinkFactory", PLUGIN_NAME));
                    }
                    OptionRule optionRule = FactoryUtil.sinkFullOptionRule((TableSinkFactory) factories.get(0));
                    saveModeSink.checkOptions(sinkConfig, optionRule);
                }

                seaTunnelSink.prepare(sinkConfig);
                seaTunnelSink.setJobContext(jobContext);

                return seaTunnelSink;
            }).distinct().collect(Collectors.toList());
        return new ImmutablePair<>(sinks, pluginJars);
    }
}

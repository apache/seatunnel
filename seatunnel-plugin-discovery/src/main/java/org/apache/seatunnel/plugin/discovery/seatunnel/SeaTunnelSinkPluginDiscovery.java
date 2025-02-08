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

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;

import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;

public class SeaTunnelSinkPluginDiscovery extends AbstractPluginDiscovery<SeaTunnelSink> {

    private static final String MULTITABLESINK_FACTORYIDENTIFIER = "MultiTableSink";

    public SeaTunnelSinkPluginDiscovery() {
        super();
    }

    @Override
    public ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> getOptionRules(
            String pluginIdentifier) {
        return super.getOptionRules(pluginIdentifier);
    }

    @Override
    public LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {

        LinkedHashMap<PluginIdentifier, OptionRule> plugins = new LinkedHashMap<>();
        getPluginFactories().stream()
                .filter(
                        pluginFactory ->
                                !pluginFactory
                                                .factoryIdentifier()
                                                .equals(MULTITABLESINK_FACTORYIDENTIFIER)
                                        && TableSinkFactory.class.isAssignableFrom(
                                                pluginFactory.getClass()))
                .forEach(
                        pluginFactory ->
                                getPluginsByFactoryIdentifier(
                                        plugins,
                                        PluginType.SINK,
                                        pluginFactory.factoryIdentifier(),
                                        FactoryUtil.sinkFullOptionRule(
                                                (TableSinkFactory) pluginFactory)));
        return plugins;
    }

    public SeaTunnelSinkPluginDiscovery(BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super(addURLToClassLoader);
    }

    @Override
    protected Class<SeaTunnelSink> getPluginBaseClass() {
        return SeaTunnelSink.class;
    }
}

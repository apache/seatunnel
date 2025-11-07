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
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;

import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.LinkedHashMap;
import java.util.List;

public class SeaTunnelTransformPluginDiscovery extends AbstractPluginDiscovery<SeaTunnelTransform> {

    public SeaTunnelTransformPluginDiscovery() {
        super(Common.connectorDir());
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
                                TableTransformFactory.class.isAssignableFrom(
                                        pluginFactory.getClass()))
                .forEach(
                        pluginFactory ->
                                getPluginsByFactoryIdentifier(
                                        plugins,
                                        PluginType.TRANSFORM,
                                        pluginFactory.factoryIdentifier(),
                                        pluginFactory.optionRule()));
        return plugins;
    }

    @Override
    protected Class<SeaTunnelTransform> getPluginBaseClass() {
        return SeaTunnelTransform.class;
    }
}

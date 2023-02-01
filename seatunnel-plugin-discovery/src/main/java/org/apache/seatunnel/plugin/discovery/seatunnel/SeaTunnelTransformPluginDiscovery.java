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
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SeaTunnelTransformPluginDiscovery extends AbstractPluginDiscovery<SeaTunnelTransform> {

    public static String PLUGIN_TYPE = PluginType.TRANSFORM.getType();

    public SeaTunnelTransformPluginDiscovery() {
        super(Common.libDir());
    }

    @Override
    protected Class<SeaTunnelTransform> getPluginBaseClass() {
        return SeaTunnelTransform.class;
    }

    public ImmutablePair<List<SeaTunnelTransform>, Set<URL>> initializePlugins(List<URL> jarForCreatePluginInstance,
                                                                               List<? extends Config> pluginConfigs,
                                                                               JobContext jobContext) {
        Set<URL> pluginJars = new HashSet<>();
        List<SeaTunnelTransform> transforms = pluginConfigs.stream()
            .map(transformConfig -> {
                PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of(AbstractPluginDiscovery.ENGINE_TYPE, PLUGIN_TYPE,
                        transformConfig.getString(AbstractPluginDiscovery.PLUGIN_NAME));
                pluginJars.addAll(getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
                SeaTunnelTransform pluginInstance;
                if (CollectionUtils.isEmpty(jarForCreatePluginInstance)) {
                    pluginInstance = createPluginInstance(pluginIdentifier);
                } else {
                    pluginInstance = createPluginInstance(pluginIdentifier, jarForCreatePluginInstance);
                }
                pluginInstance.prepare(transformConfig);
                pluginInstance.setJobContext(jobContext);
                return pluginInstance;
            }).distinct().collect(Collectors.toList());
        return new ImmutablePair<>(transforms, pluginJars);
    }
}

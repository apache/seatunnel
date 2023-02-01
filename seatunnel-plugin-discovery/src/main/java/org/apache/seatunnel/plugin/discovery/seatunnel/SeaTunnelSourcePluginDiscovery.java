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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public class SeaTunnelSourcePluginDiscovery extends AbstractPluginDiscovery<SeaTunnelSource> {
    private static final String PLUGIN_TYPE = "source";

    public SeaTunnelSourcePluginDiscovery() {
        super("seatunnel");
    }

    public SeaTunnelSourcePluginDiscovery(BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super("seatunnel", addURLToClassLoader);
    }

    @Override
    protected Class<SeaTunnelSource> getPluginBaseClass() {
        return SeaTunnelSource.class;
    }

    public ImmutablePair<List<SeaTunnelSource<?, ?, ?>>, Set<URL>> initializePlugins(List<URL> jarForCreatePluginInstance, List<? extends Config> pluginConfigs, JobContext jobContext) {
        List<SeaTunnelSource<?, ?, ?>> sources = new ArrayList<>();
        Set<URL> connectorJars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                AbstractPluginDiscovery.ENGINE_TYPE, PLUGIN_TYPE,
                sourceConfig.getString(AbstractPluginDiscovery.PLUGIN_NAME));
            connectorJars.addAll(getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SeaTunnelSource<?, ?, ?> seaTunnelSource;
            if (CollectionUtils.isEmpty(jarForCreatePluginInstance)) {
                seaTunnelSource = createPluginInstance(pluginIdentifier);
            } else {
                seaTunnelSource = createPluginInstance(pluginIdentifier, jarForCreatePluginInstance);
            }

            seaTunnelSource.prepare(sourceConfig);
            seaTunnelSource.setJobContext(jobContext);
            if (jobContext.getJobMode() == JobMode.BATCH
                && seaTunnelSource.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
                throw new UnsupportedOperationException(String.format("'%s' source don't support off-line job.", seaTunnelSource.getPluginName()));
            }
            sources.add(seaTunnelSource);
        }
        return new ImmutablePair<>(sources, connectorJars);
    }
}

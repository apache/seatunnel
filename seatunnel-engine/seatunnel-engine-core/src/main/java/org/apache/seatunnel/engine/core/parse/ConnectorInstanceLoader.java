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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.lang3.tuple.ImmutablePair;

import scala.Serializable;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConnectorInstanceLoader {
    private ConnectorInstanceLoader() {
        throw new IllegalStateException("Utility class");
    }

    public static ImmutablePair<SeaTunnelSource, Set<URL>> loadSourceInstance(
            Config sourceConfig, JobContext jobContext, List<URL> pluginJars) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SOURCE_PLUGIN,
                        sourceConfig.getString(CollectionConstants.PLUGIN_NAME));

        List<URL> pluginJarPaths =
                sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));

        SeaTunnelSource seaTunnelSource =
                sourcePluginDiscovery.createPluginInstance(pluginIdentifier, pluginJars);
        return new ImmutablePair<>(seaTunnelSource, new HashSet<>(pluginJarPaths));
    }

    public static ImmutablePair<
                    SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>, Set<URL>>
            loadSinkInstance(Config sinkConfig, JobContext jobContext, List<URL> pluginJars) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SINK_PLUGIN,
                        sinkConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink =
                sinkPluginDiscovery.createPluginInstance(pluginIdentifier, pluginJars);
        return new ImmutablePair<>(seaTunnelSink, new HashSet<>(pluginJarPaths));
    }

    public static ImmutablePair<SeaTunnelTransform<?>, Set<URL>> loadTransformInstance(
            Config transformConfig, JobContext jobContext, List<URL> pluginJars) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.TRANSFORM_PLUGIN,
                        transformConfig.getString(CollectionConstants.PLUGIN_NAME));

        List<URL> pluginJarPaths =
                transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        SeaTunnelTransform<?> seaTunnelTransform =
                transformPluginDiscovery.createPluginInstance(pluginIdentifier, pluginJars);
        return new ImmutablePair<>(seaTunnelTransform, new HashSet<>(pluginJarPaths));
    }
}

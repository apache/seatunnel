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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.URL;
import java.util.List;

import scala.Serializable;

public class ConnectorInstanceLoader {
    private ConnectorInstanceLoader() {
        throw new IllegalStateException("Utility class");
    }

    public static ImmutablePair<SeaTunnelSource, List<URL>> loadSourceInstance(Config sourceConfig) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        PluginIdentifier pluginIdentifier = PluginIdentifier.of(
            CollectionConstants.SEATUNNEL_PLUGIN,
            CollectionConstants.SOURCE_PLUGIN,
            sourceConfig.getString(CollectionConstants.PLUGIN_NAME));

        List<URL> pluginJarPaths = sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));

        SeaTunnelSource seaTunnelSource = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
        seaTunnelSource.prepare(sourceConfig);
        seaTunnelSource.setSeaTunnelContext(SeaTunnelContext.getContext());
        if (SeaTunnelContext.getContext().getJobMode() == JobMode.BATCH
            && seaTunnelSource.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
            throw new UnsupportedOperationException(
                String.format("'%s' source don't support off-line job.", seaTunnelSource.getPluginName()));
        }
        return new ImmutablePair<>(seaTunnelSource, pluginJarPaths);
    }

    public static ImmutablePair<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>, List<URL>> loadSinkInstance(
        Config sinkConfig) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        PluginIdentifier pluginIdentifier = PluginIdentifier.of(
            CollectionConstants.SEATUNNEL_PLUGIN,
            CollectionConstants.SINK_PLUGIN,
            sinkConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths = sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink =
            sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        seaTunnelSink.prepare(sinkConfig);
        seaTunnelSink.setSeaTunnelContext(SeaTunnelContext.getContext());
        return new ImmutablePair<>(seaTunnelSink, pluginJarPaths);
    }

    public static ImmutablePair<SeaTunnelTransform<?>, List<URL>> loadTransformInstance(Config transformConfig) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery = new SeaTunnelTransformPluginDiscovery();
        PluginIdentifier pluginIdentifier = PluginIdentifier.of(
            CollectionConstants.SEATUNNEL_PLUGIN,
            CollectionConstants.TRANSFORM_PLUGIN,
            transformConfig.getString(CollectionConstants.PLUGIN_NAME));

        List<URL> pluginJarPaths = transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        SeaTunnelTransform<?> seaTunnelTransform =
                transformPluginDiscovery.createPluginInstance(pluginIdentifier);
        return new ImmutablePair<>(seaTunnelTransform, pluginJarPaths);
    }
}

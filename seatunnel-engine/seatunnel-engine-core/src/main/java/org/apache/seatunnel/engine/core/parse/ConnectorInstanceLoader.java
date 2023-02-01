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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ConnectorInstanceLoader {
    private ConnectorInstanceLoader() {
        throw new IllegalStateException("Utility class");
    }

    public static ImmutablePair<SeaTunnelSource, Set<URL>> loadSourceInstance(
        Config sourceConfig, JobContext jobContext, List<URL> pluginJars) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        ImmutablePair<List<SeaTunnelSource<?, ?, ?>>, Set<URL>> listSetImmutablePair =
            sourcePluginDiscovery.initializePlugins(pluginJars, Arrays.asList(sourceConfig), jobContext);
        return new ImmutablePair<>(listSetImmutablePair.getLeft().get(0), listSetImmutablePair.getRight());
    }

    public static ImmutablePair<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>, Set<URL>> loadSinkInstance(
        Config sinkConfig, JobContext jobContext, List<URL> pluginJars) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        ImmutablePair<List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>>, Set<URL>>
            listSetImmutablePair =
            sinkPluginDiscovery.initializePlugins(pluginJars, Arrays.asList(sinkConfig), jobContext);
        return new ImmutablePair<>(listSetImmutablePair.getLeft().get(0), listSetImmutablePair.getRight());
    }

    public static ImmutablePair<SeaTunnelTransform<?>, Set<URL>> loadTransformInstance(
        Config transformConfig, JobContext jobContext, List<URL> pluginJars) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery = new SeaTunnelTransformPluginDiscovery();
        ImmutablePair<List<SeaTunnelTransform>, Set<URL>> listSetImmutablePair =
            transformPluginDiscovery.initializePlugins(pluginJars, Arrays.asList(transformConfig), jobContext);
        return new ImmutablePair<>(listSetImmutablePair.getLeft().get(0), listSetImmutablePair.getRight());
    }
}

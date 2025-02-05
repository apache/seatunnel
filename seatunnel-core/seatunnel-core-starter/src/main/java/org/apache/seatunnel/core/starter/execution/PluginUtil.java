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

package org.apache.seatunnel.core.starter.execution;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryException;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import java.net.URL;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;

/** The util used for Spark/Flink to create to SeaTunnelSource etc. */
@SuppressWarnings("rawtypes")
public class PluginUtil {

    protected static final String ENGINE_TYPE = "seatunnel";

    public static Optional<? extends Factory> createTransformFactory(
            SeaTunnelFactoryDiscovery factoryDiscovery,
            SeaTunnelTransformPluginDiscovery transformPluginDiscovery,
            Config transformConfig,
            List<URL> pluginJars) {
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        ENGINE_TYPE, "transform", transformConfig.getString(PLUGIN_NAME.key()));
        pluginJars.addAll(
                transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
        try {
            return factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        } catch (FactoryException e) {
            return Optional.empty();
        }
    }

    public static Optional<? extends Factory> createSinkFactory(
            SeaTunnelFactoryDiscovery factoryDiscovery,
            SeaTunnelSinkPluginDiscovery sinkPluginDiscovery,
            Config sinkConfig,
            List<URL> pluginJars) {
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(ENGINE_TYPE, "sink", sinkConfig.getString(PLUGIN_NAME.key()));
        pluginJars.addAll(
                sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
        try {
            return factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        } catch (FactoryException e) {
            return Optional.empty();
        }
    }

    public static void ensureJobModeMatch(JobContext jobContext, SeaTunnelSource source) {
        if (jobContext.getJobMode() == JobMode.BATCH
                && source.getBoundedness()
                        == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
            throw new UnsupportedOperationException(
                    String.format(
                            "'%s' source don't support off-line job.", source.getPluginName()));
        }
    }
}

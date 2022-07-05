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

package org.apache.seatunnel.plugin.discovery;

import org.apache.seatunnel.common.config.Common;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import java.net.URL;
import java.util.List;

/**
 * Plugins discovery interface, used to find plugin. Each plugin type should have its own implementation.
 *
 * @param <T> plugin type
 */
public interface PluginDiscovery<T> {

    String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";
    /**
     * The plugin mapping config.
     * e,g.flink.source.DruidSource=seatunnel-connector-flink-druid
     */
    Config PLUGIN_JAR_MAPPING =
        ConfigFactory
            // todo: rename to plugin dir
            .parseFile(Common.connectorDir().resolve(PLUGIN_MAPPING_FILE).toFile())
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

    /**
     * Get all plugin jar paths.
     *
     * @return plugin jars.
     */
    List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers);

    /**
     * Get plugin instance by plugin identifier.
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin instance. If not found, throw IllegalArgumentException.
     */
    T createPluginInstance(PluginIdentifier pluginIdentifier);

    /**
     * Get all plugin instances.
     *
     * @return plugin instances.
     */
    List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers);

}

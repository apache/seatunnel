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

package org.apache.seatunnel.config;

import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.BaseFlinkTransform;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.spark.BaseSparkSink;
import org.apache.seatunnel.spark.BaseSparkSource;
import org.apache.seatunnel.spark.BaseSparkTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Used to load the plugins.
 *
 * @param <ENVIRONMENT> environment
 */
public class PluginFactory<ENVIRONMENT extends RuntimeEnv> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PluginFactory.class);
    private final Config config;
    private final EngineType engineType;
    private static final Map<EngineType, Map<PluginType, Class<?>>> PLUGIN_BASE_CLASS_MAP;

    private static final String PLUGIN_NAME_KEY = "plugin_name";

    static {
        PLUGIN_BASE_CLASS_MAP = new HashMap<>();
        Map<PluginType, Class<?>> sparkBaseClassMap = new HashMap<>();
        sparkBaseClassMap.put(PluginType.SOURCE, BaseSparkSource.class);
        sparkBaseClassMap.put(PluginType.TRANSFORM, BaseSparkTransform.class);
        sparkBaseClassMap.put(PluginType.SINK, BaseSparkSink.class);
        PLUGIN_BASE_CLASS_MAP.put(EngineType.SPARK, sparkBaseClassMap);

        Map<PluginType, Class<?>> flinkBaseClassMap = new HashMap<>();
        flinkBaseClassMap.put(PluginType.SOURCE, BaseFlinkSource.class);
        flinkBaseClassMap.put(PluginType.TRANSFORM, BaseFlinkTransform.class);
        flinkBaseClassMap.put(PluginType.SINK, BaseFlinkSink.class);
        PLUGIN_BASE_CLASS_MAP.put(EngineType.FLINK, flinkBaseClassMap);
    }

    public PluginFactory(Config config, EngineType engineType) {
        this.config = config;
        this.engineType = engineType;
    }

    /**
     * Create the plugins by plugin type.
     *
     * @param type   plugin type
     * @param <T>    plugin
     * @return plugin list.
     */
    @SuppressWarnings("unchecked")
    public <T extends Plugin<ENVIRONMENT>> List<T> createPlugins(PluginType type) {
        Objects.requireNonNull(type, "PluginType can not be null when create plugins!");
        List<T> basePluginList = new ArrayList<>();
        List<? extends Config> configList = config.getConfigList(type.getType());
        configList.forEach(plugin -> {
            try {
                T t = (T) createPluginInstanceIgnoreCase(type, plugin.getString(PLUGIN_NAME_KEY));
                t.setConfig(plugin);
                basePluginList.add(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return basePluginList;
    }

    /**
     * create plugin class instance, ignore case.
     **/
    @SuppressWarnings("unchecked")
    private Plugin<?> createPluginInstanceIgnoreCase(PluginType pluginType, String pluginName) throws Exception {
        Class<Plugin<?>> pluginBaseClass = (Class<Plugin<?>>) getPluginBaseClass(engineType, pluginType);

        if (pluginName.split("\\.").length != 1) {
            // canonical class name
            Class<Plugin<?>> pluginClass = (Class<Plugin<?>>) Class.forName(pluginName);
            if (pluginClass.isAssignableFrom(pluginBaseClass)) {
                throw new IllegalArgumentException("plugin: " + pluginName + " is not extends from " + pluginBaseClass);
            }
            return pluginClass.getDeclaredConstructor().newInstance();
        }

        ServiceLoader<Plugin<?>> plugins = ServiceLoader.load(pluginBaseClass);
        for (Iterator<Plugin<?>> it = plugins.iterator(); it.hasNext(); ) {
            try {
                Plugin<?> plugin = it.next();
                if (StringUtils.equalsIgnoreCase(plugin.getPluginName(), pluginName)) {
                    return plugin;
                }
            } catch (ServiceConfigurationError e) {
                // Iterator.next() may throw ServiceConfigurationError,
                // but maybe caused by a not used plugin in this job
                LOGGER.warn("Error when load plugin: [{}]", pluginName, e);
            }
        }
        throw new ClassNotFoundException("Plugin class not found by name :[" + pluginName + "]");
    }

    private Class<?> getPluginBaseClass(EngineType engineType, PluginType pluginType) {
        if (!PLUGIN_BASE_CLASS_MAP.containsKey(engineType)) {
            throw new IllegalStateException("PluginType not support : [" + pluginType + "]");
        }
        Map<PluginType, Class<?>> pluginTypeClassMap = PLUGIN_BASE_CLASS_MAP.get(engineType);
        if (!pluginTypeClassMap.containsKey(pluginType)) {
            throw new IllegalStateException(pluginType + " is not supported in engine " + engineType);
        }
        return pluginTypeClassMap.get(pluginType);
    }

}

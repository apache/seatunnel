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

import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.BaseFlinkTransform;
import org.apache.seatunnel.spark.BaseSparkSink;
import org.apache.seatunnel.spark.BaseSparkSource;
import org.apache.seatunnel.spark.BaseSparkTransform;

import java.util.HashMap;
import java.util.Map;

public enum EngineType {

    SPARK("spark", new HashMap<PluginType, Class<?>>() {
        {
            put(PluginType.SOURCE, BaseSparkSource.class);
            put(PluginType.TRANSFORM, BaseSparkTransform.class);
            put(PluginType.SINK, BaseSparkSink.class);
        }
    }),

    FLINK("flink", new HashMap<PluginType, Class<?>>() {
        {
            put(PluginType.SOURCE, BaseFlinkSource.class);
            put(PluginType.TRANSFORM, BaseFlinkTransform.class);
            put(PluginType.SINK, BaseFlinkSink.class);
        }
    }),
    ;

    private final String engine;
    /**
     * store the map of plugin type to plugin base class, used to load the plugin.
     */
    private final Map<PluginType, Class<?>> pluginTypes;

    EngineType(String engine, Map<PluginType, Class<?>> pluginTypes) {
        this.engine = engine;
        this.pluginTypes = pluginTypes;
    }

    public String getEngine() {
        return engine;
    }

    public Map<PluginType, Class<?>> getPluginTypes() {
        return pluginTypes;
    }
}

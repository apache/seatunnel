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

package org.apache.seatunnel.core.flink.config;

import org.apache.seatunnel.core.base.config.ConfigChecker;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.config.EnvironmentFactory;
import org.apache.seatunnel.core.base.config.PluginFactory;
import org.apache.seatunnel.core.base.config.PluginType;
import org.apache.seatunnel.core.base.exception.ConfigCheckException;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

public class FlinkApiConfigChecker implements ConfigChecker<FlinkEnvironment> {

    @Override
    public void checkConfig(Config config) throws ConfigCheckException {
        try {
            // check environment
            FlinkEnvironment environment = new EnvironmentFactory<FlinkEnvironment>(config, EngineType.FLINK).getEnvironment();
            // check plugins
            PluginFactory<FlinkEnvironment> pluginFactory = new PluginFactory<>(config, EngineType.FLINK);
            pluginFactory.createPlugins(PluginType.SOURCE);
            pluginFactory.createPlugins(PluginType.TRANSFORM);
            pluginFactory.createPlugins(PluginType.SINK);
        } catch (Exception ex) {
            throw new ConfigCheckException("Config check fail", ex);
        }
    }
}

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

import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.env.RuntimeEnv;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;

/**
 * Used to build the {@link  Config} from file.
 *
 * @param <ENVIRONMENT> environment type.
 */
public class ConfigBuilder<ENVIRONMENT extends RuntimeEnv> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBuilder.class);

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final Path configFile;
    private final EngineType engine;
    private final Config config;

    public ConfigBuilder(Path configFile, EngineType engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.config = load();
    }

    private Config load() {

        if (configFile == null) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        LOGGER.info("Loading config file: {}", configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
            .parseFile(configFile.toFile())
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(),
                ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        LOGGER.info("parsed config file: {}", config.root().render(options));
        return config;
    }

    public Config getConfig() {
        return config;
    }

    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        // check environment
        ENVIRONMENT environment = new EnvironmentFactory<ENVIRONMENT>(config, engine).getEnvironment();
        // check plugins
        PluginFactory<ENVIRONMENT> pluginFactory = new PluginFactory<>(config, engine);
        pluginFactory.createPlugins(PluginType.SOURCE);
        pluginFactory.createPlugins(PluginType.TRANSFORM);
        pluginFactory.createPlugins(PluginType.SINK);
    }
}

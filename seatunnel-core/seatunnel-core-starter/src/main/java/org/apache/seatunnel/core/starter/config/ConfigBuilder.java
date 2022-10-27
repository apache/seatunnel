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

package org.apache.seatunnel.core.starter.config;

import org.apache.seatunnel.common.config.ConfigRuntimeException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

/**
 * Used to build the {@link  Config} from file.
 */
@Slf4j
public class ConfigBuilder {

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final Path configFile;
    private final Config config;

    public ConfigBuilder(Path configFile) {
        this.configFile = configFile;
        this.config = load();
    }

    private Config load() {

        if (configFile == null) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        log.info("Loading config file: {}", configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
            .parseFile(configFile.toFile())
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(),
                ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        log.info("parsed config file: {}", config.root().render(options));
        return config;
    }

    public Config getConfig() {
        return config;
    }

}

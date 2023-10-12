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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.api.configuration.ConfigAdapter;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

/** Used to build the {@link Config} from config file. */
@Slf4j
public class ConfigBuilder {

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS =
            ConfigRenderOptions.concise().setFormatted(true);

    private ConfigBuilder() {
        // utility class and cannot be instantiated
    }

    private static Config ofInner(@NonNull Path filePath) {
        Config config =
                ConfigFactory.parseFile(filePath.toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return ConfigShadeUtils.decryptConfig(config);
    }

    public static Config of(@NonNull String filePath) {
        Path path = Paths.get(filePath);
        return of(path);
    }

    public static Config of(@NonNull Path filePath) {
        log.info("Loading config file from path: {}", filePath);
        Optional<ConfigAdapter> adapterSupplier = ConfigAdapterUtils.selectAdapter(filePath);
        Config config =
                adapterSupplier
                        .map(adapter -> of(adapter, filePath))
                        .orElseGet(() -> ofInner(filePath));
        log.info("Parsed config file: {}", config.root().render(CONFIG_RENDER_OPTIONS));
        return config;
    }

    public static Config of(@NonNull Map<String, Object> objectMap) {
        log.info("Loading config file from objectMap");
        Config config = ConfigFactory.parseMap(objectMap);
        return ConfigShadeUtils.decryptConfig(config);
    }

    public static Config of(@NonNull ConfigAdapter configAdapter, @NonNull Path filePath) {
        log.info("With config adapter spi {}", configAdapter.getClass().getName());
        try {
            Map<String, Object> flattenedMap = configAdapter.loadConfig(filePath);
            Config config = ConfigFactory.parseMap(flattenedMap);
            return ConfigShadeUtils.decryptConfig(config);
        } catch (Exception warn) {
            log.warn(
                    "Loading config failed with spi {}, fallback to HOCON loader.",
                    configAdapter.getClass().getName());
            return ofInner(filePath);
        }
    }
}

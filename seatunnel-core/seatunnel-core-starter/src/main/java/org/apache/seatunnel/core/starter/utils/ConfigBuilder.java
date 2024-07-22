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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.impl.Parseable;

import org.apache.seatunnel.api.configuration.ConfigAdapter;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.ParserException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.core.starter.utils.ConfigShadeUtils.DEFAULT_SENSITIVE_KEYWORDS;

/** Used to build the {@link Config} from config file. */
@Slf4j
public class ConfigBuilder {

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS =
            ConfigRenderOptions.concise().setFormatted(true);

    private ConfigBuilder() {
        // utility class and cannot be instantiated
    }

    private static Config ofInner(@NonNull Path filePath, List<String> variables) {
        Config config =
                ConfigFactory.parseFile(filePath.toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
    }

    public static Config of(@NonNull String filePath) {
        Path path = Paths.get(filePath);
        return of(path);
    }

    public static Config of(@NonNull String filePath, List<String> variables) {
        Path path = Paths.get(filePath);
        return of(path, variables);
    }

    public static Config of(@NonNull Path filePath) {
        return of(filePath, null);
    }

    public static Config of(@NonNull Path filePath, List<String> variables) {
        log.info("Loading config file from path: {}", filePath);
        Optional<ConfigAdapter> adapterSupplier = ConfigAdapterUtils.selectAdapter(filePath);
        Config config =
                adapterSupplier
                        .map(adapter -> of(adapter, filePath, variables))
                        .orElseGet(() -> ofInner(filePath, variables));
        boolean isJson = filePath.getFileName().toString().endsWith(".json");
        log.info(
                "Parsed config file: \n{}",
                mapToString(configDesensitization(config.root().unwrapped()), isJson));
        return config;
    }

    public static Config of(@NonNull Map<String, Object> objectMap) {
        return of(objectMap, false, false);
    }

    public static Config of(
            @NonNull Map<String, Object> objectMap, boolean isEncrypt, boolean isJson) {
        log.info("Loading config file from objectMap");
        Config config =
                ConfigFactory.parseMap(objectMap)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        if (!isEncrypt) {
            config = ConfigShadeUtils.decryptConfig(config);
        }
        log.info(
                "Parsed config file: \n{}",
                mapToString(configDesensitization(config.root().unwrapped()), isJson));
        return config;
    }

    public static Map<String, Object> configDesensitization(Map<String, Object> configMap) {
        return configMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    String key = entry.getKey();
                                    if (Arrays.asList(DEFAULT_SENSITIVE_KEYWORDS)
                                            .contains(key.toLowerCase())) {
                                        return "******";
                                    }
                                    Object value = entry.getValue();
                                    if (value instanceof Map) {
                                        if ("schema".equals(key)) {
                                            return value;
                                        }
                                        return configDesensitization((Map<String, Object>) value);
                                    } else if (value instanceof List) {
                                        return ((List<?>) value)
                                                .stream()
                                                        .map(
                                                                v -> {
                                                                    if (v instanceof Map) {
                                                                        return configDesensitization(
                                                                                (Map<
                                                                                                String,
                                                                                                Object>)
                                                                                        v);
                                                                    }
                                                                    return v;
                                                                })
                                                        .collect(Collectors.toList());
                                    }
                                    return value;
                                }));
    }

    public static Config of(
            @NonNull ConfigAdapter configAdapter, @NonNull Path filePath, List<String> variables) {
        log.info("With config adapter spi {}", configAdapter.getClass().getName());
        try {
            Map<String, Object> flattenedMap = configAdapter.loadConfig(filePath);
            Config config = ConfigFactory.parseMap(flattenedMap);
            return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
        } catch (ParserException | IllegalArgumentException e) {
            throw e;
        } catch (Exception warn) {
            log.warn(
                    "Loading config failed with spi {}, fallback to HOCON loader.",
                    configAdapter.getClass().getName());
            return ofInner(filePath, variables);
        }
    }

    private static Config backfillUserVariables(Config config, List<String> variables) {
        if (variables != null) {
            variables.stream()
                    .filter(Objects::nonNull)
                    .map(variable -> variable.split("=", 2))
                    .filter(pair -> pair.length == 2)
                    .forEach(pair -> System.setProperty(pair[0], pair[1]));
            Config systemConfig =
                    Parseable.newProperties(
                                    System.getProperties(),
                                    ConfigParseOptions.defaults()
                                            .setOriginDescription("system properties"))
                            .parse()
                            .toConfig();
            return config.resolveWith(
                    systemConfig, ConfigResolveOptions.defaults().setAllowUnresolved(true));
        }
        return config;
    }

    public static String mapToString(Map<String, Object> configMap, boolean isJson) {
        ConfigRenderOptions configRenderOptions =
                ConfigRenderOptions.concise().setFormatted(true).setJson(isJson);
        ConfigParseOptions configParseOptions =
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON);
        if (!isJson) {
            convertHoconMap(configMap);
            configParseOptions.setSyntax(ConfigSyntax.CONF);
        }
        Config config =
                ConfigFactory.parseString(JsonUtils.toJsonString(configMap), configParseOptions)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return config.root().render(configRenderOptions);
    }

    private static void convertHoconMap(Map<String, Object> configMap) {
        convertField(configMap, "source");
        convertField(configMap, "sink");
    }

    private static void convertField(Map<String, Object> configMap, String fieldName) {
        if (configMap.containsKey(fieldName)) {
            Object fieldValue = configMap.get(fieldName);
            if (fieldValue instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> list = (List<Map<String, Object>>) fieldValue;
                Map<String, Object> newMap =
                        list.stream()
                                .collect(
                                        HashMap::new,
                                        (m, entry) -> {
                                            String pluginName =
                                                    entry.getOrDefault("plugin_name", "")
                                                            .toString();
                                            Map<String, Object> pluginConfig = new HashMap<>(entry);
                                            pluginConfig.remove("plugin_name");
                                            m.put(pluginName, pluginConfig);
                                        },
                                        HashMap::putAll);
                configMap.put(fieldName, newMap);
            }
        }
    }
}

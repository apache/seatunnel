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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.com.typesafe.config.impl.Parseable;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ConfigAdapter;
import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.ParserException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.utils.PlaceholderUtils.extractPlaceholderKeys;
import static org.apache.seatunnel.common.utils.PlaceholderUtils.replaceAllPlaceholders;

/** Used to build the {@link Config} from config file. */
@Slf4j
public class ConfigBuilder {

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS =
            ConfigRenderOptions.concise().setFormatted(true);

    private static final String MASKED_VALUE = "******";
    private static final String CONFIG_PATH_SEPARATOR = ".";
    // Treat common option separators as equivalent when matching config paths in logs.
    private static final Pattern CONFIG_OPTION_SEPARATOR_PATTERN = Pattern.compile("[._-]+");

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
        log.info(
                "Parsed config file: \n{}",
                mapToString(
                        configDesensitization(
                                config.root().unwrapped(),
                                ConfigShadeUtils.getLogDesensitizationOptions(config))));
        return config;
    }

    public static Config of(@NonNull Map<String, Object> objectMap) {
        log.info("Loading config file from objectMap");
        Config config =
                ConfigFactory.parseMap(objectMap)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        log.info(
                "Parsed config file: \n{}",
                mapToString(
                        configDesensitization(
                                config.root().unwrapped(),
                                ConfigShadeUtils.getLogDesensitizationOptions(config))));
        return config;
    }

    public static Map<String, Object> configDesensitization(
            Map<String, Object> configMap, Set<String> sensitiveKeywords) {
        Set<String> normalizedSensitiveKeywords =
                sensitiveKeywords.stream()
                        .map(ConfigBuilder::normalizeConfigOption)
                        .collect(Collectors.toSet());
        return configDesensitization(configMap, normalizedSensitiveKeywords, null);
    }

    /**
     * Recursively builds a masked copy of the config map.
     *
     * <p>The accumulated {@code parentPath} preserves dotted option context after HOCON has
     * expanded paths into nested maps.
     */
    private static Map<String, Object> configDesensitization(
            Map<String, Object> configMap,
            Set<String> normalizedSensitiveKeywords,
            String parentPath) {
        return configMap.entrySet().stream()
                .collect(
                        LinkedHashMap::new,
                        (m, p) -> {
                            String key = p.getKey();
                            Object value = p.getValue();
                            String configPath =
                                    parentPath == null
                                            ? key
                                            : parentPath + CONFIG_PATH_SEPARATOR + key;
                            if (isSensitiveOption(key, configPath, normalizedSensitiveKeywords)) {
                                if (value instanceof List<?>) {
                                    List<Object> maskedList =
                                            ((List<?>) value)
                                                    .stream()
                                                            .map(v -> MASKED_VALUE)
                                                            .collect(Collectors.toList());
                                    m.put(key, maskedList);
                                } else {
                                    m.put(key, MASKED_VALUE);
                                }
                            } else if (value instanceof String
                                    && ((String) value)
                                            .regionMatches(true, 0, "jdbc:", 0, "jdbc:".length())) {
                                m.put(key, MASKED_VALUE);
                            } else {
                                if (value instanceof Map<?, ?>) {
                                    m.put(
                                            key,
                                            configDesensitization(
                                                    (Map<String, Object>) value,
                                                    normalizedSensitiveKeywords,
                                                    configPath));
                                } else if (value instanceof List<?>) {
                                    List<?> listValue = (List<?>) value;
                                    List<Object> newList =
                                            listValue.stream()
                                                    .map(
                                                            v -> {
                                                                if (v instanceof Map<?, ?>) {
                                                                    return configDesensitization(
                                                                            (Map<String, Object>) v,
                                                                            normalizedSensitiveKeywords,
                                                                            configPath);
                                                                } else {
                                                                    return v;
                                                                }
                                                            })
                                                    .collect(Collectors.toList());
                                    m.put(key, newList);
                                } else {
                                    m.put(key, value);
                                }
                            }
                        },
                        LinkedHashMap::putAll);
    }

    /**
     * Checks whether the current option should be masked in the parsed-config log.
     *
     * <p>The matcher compares both the leaf key and the accumulated config path. Option separators
     * '.', '_' and '-' are treated as equivalent, so paths like {@code
     * kafka.config.sasl.jaas.config} can match {@code sasl.jaas.config}. Suffix matching is applied
     * only to multi-segment sensitive options such as {@code access_key}; single-word options such
     * as {@code token} still require an exact leaf-key or full-path match.
     */
    private static boolean isSensitiveOption(
            String key, String configPath, Set<String> normalizedSensitiveKeywords) {
        String normalizedKey = normalizeConfigOption(key);
        String normalizedConfigPath = normalizeConfigOption(configPath);
        if (normalizedSensitiveKeywords.contains(normalizedKey)
                || normalizedSensitiveKeywords.contains(normalizedConfigPath)) {
            return true;
        }
        return normalizedSensitiveKeywords.stream()
                .filter(ConfigBuilder::isMultiSegmentOption)
                .anyMatch(
                        sensitiveKeyword -> normalizedConfigPath.endsWith("_" + sensitiveKeyword));
    }

    /**
     * Normalizes common option separator styles so equivalent config names can share one matching
     * rule.
     */
    private static String normalizeConfigOption(String option) {
        return CONFIG_OPTION_SEPARATOR_PATTERN.matcher(option.toLowerCase()).replaceAll("_");
    }

    private static boolean isMultiSegmentOption(String option) {
        return option.contains("_");
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
            Map<String, Object> userConfigMap =
                    variables.stream()
                            .filter(Objects::nonNull)
                            .map(variable -> variable.split("=", 2))
                            .filter(pair -> pair.length == 2)
                            .peek(
                                    pair -> {
                                        if (TablePlaceholder.isSystemPlaceholder(pair[0])) {
                                            throw new ConfigCheckException(
                                                    "System placeholders cannot be used. Incorrect config parameter: "
                                                            + pair[0]);
                                        }
                                    })
                            .collect(
                                    Collectors.toMap(
                                            pair -> pair[0],
                                            pair -> parseUserValue(pair[1]).unwrapped()));

            Config userConfig = ConfigFactory.parseMap(userConfigMap);

            Config systemConfig =
                    Parseable.newProperties(
                                    System.getProperties(),
                                    ConfigParseOptions.defaults()
                                            .setOriginDescription("system properties"))
                            .parse()
                            .toConfig();

            Config sourceConfig = userConfig.withFallback(systemConfig);

            Set<String> placeholders = new LinkedHashSet<>();
            Config processedConfig =
                    processConfigObject(config.root(), sourceConfig, placeholders).toConfig();

            Config cleanSourceConfig = filterSourceConfig(sourceConfig, placeholders);

            Config resolvedConfig =
                    processedConfig
                            .withFallback(cleanSourceConfig)
                            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));

            // remove userConfig
            for (String key : placeholders) {
                if (!config.hasPath(key) && resolvedConfig.hasPath(key)) {
                    resolvedConfig = resolvedConfig.withoutPath(key);
                }
            }

            return resolvedConfig;
        }
        return config;
    }

    private static ConfigValue parseUserValue(String value) {
        if (value == null) {
            return ConfigValueFactory.fromAnyRef(null);
        }

        if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
            value = StringUtils.unwrap(value, "\"");
            return ConfigValueFactory.fromAnyRef(value);
        }

        boolean maybeJsonOrArray =
                (value.startsWith("{") && value.endsWith("}"))
                        || (value.startsWith("[") && value.endsWith("]"));

        if (maybeJsonOrArray) {
            try {
                Config parsed = ConfigFactory.parseString("v = " + value);
                return parsed.root().get("v");
            } catch (Exception e) {
                log.warn(
                        "Value '{}' looks like JSON or Array but failed to parse. "
                                + "fallback to plain string. "
                                + "If you intended to pass a Map/List, please check the format. Error: {}",
                        value,
                        e.getMessage());

                return ConfigValueFactory.fromAnyRef(value);
            }
        }

        return ConfigValueFactory.fromAnyRef(value);
    }

    private static ConfigObject processConfigObject(
            ConfigObject obj, Config sourceConfig, Set<String> placeholders) {

        ConfigObject result = obj;

        for (Map.Entry<String, ConfigValue> entry : obj.entrySet()) {
            String key = entry.getKey();
            ConfigValue value = entry.getValue();
            ConfigValue processed = processConfigValue(value, sourceConfig, placeholders);

            if (processed != value) {
                result = result.withValue(key, processed);
            }
        }
        return result;
    }

    private static ConfigList processConfigList(
            ConfigList list, Config sourceConfig, Set<String> placeholders) {
        List<ConfigValue> values = new ArrayList<>();
        boolean changed = false;

        for (ConfigValue value : list) {
            ConfigValue processed = processConfigValue(value, sourceConfig, placeholders);
            values.add(processed);
            changed |= (processed != value);
        }

        return changed ? ConfigValueFactory.fromIterable(values) : list;
    }

    private static ConfigValue processConfigValue(
            ConfigValue value, Config sourceConfig, Set<String> placeholders) {

        if (value instanceof ConfigObject) {
            return processConfigObject((ConfigObject) value, sourceConfig, placeholders);
        } else if (value instanceof ConfigList) {
            return processConfigList((ConfigList) value, sourceConfig, placeholders);
        } else {
            return processLeafValue(value, sourceConfig, placeholders);
        }
    }

    private static ConfigValue processLeafValue(
            ConfigValue value, Config sourceConfig, Set<String> placeholders) {

        try {
            Object unwrapped = value.unwrapped();
            if (unwrapped instanceof String) {
                String strValue = (String) unwrapped;
                Set<String> keys = extractPlaceholderKeys(strValue);
                for (String key : keys) {
                    if (!TablePlaceholder.isSystemPlaceholder(key)) {
                        placeholders.add(key);
                    }
                }
                String processed =
                        replaceAllPlaceholders(
                                strValue,
                                pureKey -> {
                                    if (TablePlaceholder.isSystemPlaceholder(pureKey)) {
                                        return null;
                                    }
                                    if (sourceConfig.hasPath(pureKey)) {
                                        Object val = sourceConfig.getValue(pureKey).unwrapped();
                                        return convertToString(val);
                                    }
                                    return null;
                                });

                if (!processed.equals(strValue)) {
                    ConfigValue parsed = parseUserValue(processed);
                    return ConfigValueFactory.fromAnyRef(
                            parsed.unwrapped(), value.origin().toString());
                }
            }
        } catch (ConfigException.NotResolved e) {
            String rendered = value.render(ConfigRenderOptions.concise());
            Set<String> keys = extractPlaceholderKeys(rendered);
            for (String key : keys) {
                if (!TablePlaceholder.isSystemPlaceholder(key)) {
                    placeholders.add(key);
                }
            }
        }
        return value;
    }

    private static Config filterSourceConfig(Config sourceConfig, Set<String> placeholders) {

        Config cleanSourceConfig = ConfigFactory.empty();

        for (String key : placeholders) {
            if (sourceConfig.hasPath(key)) {
                cleanSourceConfig =
                        cleanSourceConfig.withFallback(sourceConfig.getValue(key).atPath(key));
            }
        }

        return cleanSourceConfig;
    }

    private static String convertToString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }

        return ConfigValueFactory.fromAnyRef(value).render(ConfigRenderOptions.concise());
    }

    public static String mapToString(Map<String, Object> configMap) {
        ConfigParseOptions configParseOptions =
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON);
        Config config =
                ConfigFactory.parseString(JsonUtils.toJsonString(configMap), configParseOptions)
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return config.root().render(CONFIG_RENDER_OPTIONS);
    }
}

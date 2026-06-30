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
import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.api.metadata.MetadataOptions;
import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.ParserException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.utils.PlaceholderUtils.replacePlaceholders;

/** Used to build the {@link Config} from config file. */
@Slf4j
public class ConfigBuilder {

    public static final ConfigRenderOptions CONFIG_RENDER_OPTIONS =
            ConfigRenderOptions.concise().setFormatted(true);

    private static final String PLACEHOLDER_REGEX = "\\$\\{([^:{}]+)(?::[^}]*)?\\}";

    private ConfigBuilder() {
        // utility class and cannot be instantiated
    }

    private static Config ofInner(@NonNull Path filePath, List<String> variables) {
        Config config = parseConfigFile(filePath);
        return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
    }

    private static Config parseConfigFile(@NonNull Path filePath) {
        Config config =
                ConfigFactory.parseFile(filePath.toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return config;
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
                                ConfigShadeUtils.getSensitiveOptions(config))));
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
                                ConfigShadeUtils.getSensitiveOptions(config))));
        return config;
    }

    public static Map<String, Object> configDesensitization(
            Map<String, Object> configMap, Set<String> sensitiveKeywords) {
        return configMap.entrySet().stream()
                .collect(
                        LinkedHashMap::new,
                        (m, p) -> {
                            String key = p.getKey();
                            Object value = p.getValue();
                            if (sensitiveKeywords.contains(key.toLowerCase())) {
                                if (value instanceof List<?>) {
                                    List<Object> maskedList =
                                            ((List<?>) value)
                                                    .stream()
                                                            .map(v -> "******")
                                                            .collect(Collectors.toList());
                                    m.put(key, maskedList);
                                } else {
                                    m.put(key, "******");
                                }
                            } else {
                                if (value instanceof Map<?, ?>) {
                                    m.put(
                                            key,
                                            configDesensitization(
                                                    (Map<String, Object>) value,
                                                    sensitiveKeywords));
                                } else if (value instanceof List<?>) {
                                    List<?> listValue = (List<?>) value;
                                    List<Object> newList =
                                            listValue.stream()
                                                    .map(
                                                            v -> {
                                                                if (v instanceof Map<?, ?>) {
                                                                    return configDesensitization(
                                                                            (Map<String, Object>) v,
                                                                            sensitiveKeywords);
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
                    .peek(
                            pair -> {
                                if (TablePlaceholder.isSystemPlaceholder(pair[0])) {
                                    throw new ConfigCheckException(
                                            "System placeholders cannot be used. Incorrect config parameter: "
                                                    + pair[0]);
                                }
                            })
                    .forEach(pair -> System.setProperty(pair[0], pair[1]));
            Config systemConfig =
                    Parseable.newProperties(
                                    System.getProperties(),
                                    ConfigParseOptions.defaults()
                                            .setOriginDescription("system properties"))
                            .parse()
                            .toConfig();

            Config resolvedConfig =
                    config.resolveWith(
                            systemConfig, ConfigResolveOptions.defaults().setAllowUnresolved(true));

            Map<String, Object> configMap = resolvedConfig.root().unwrapped();

            configMap.forEach(
                    (key, value) -> {
                        if (value instanceof Map) {
                            processVariablesMap((Map<String, Object>) value);
                        } else if (value instanceof List) {
                            ((List<Map<String, Object>>) value)
                                    .forEach(map -> processVariablesMap(map));
                        }
                    });

            return ConfigFactory.parseString(
                            JsonUtils.toJsonString(configMap),
                            ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        }
        return config;
    }

    private static void processVariablesMap(Map<String, Object> mapValue) {
        mapValue.forEach(
                (innerKey, innerValue) -> {
                    if (innerValue instanceof Map) {
                        processVariablesMap((Map<String, Object>) innerValue);
                    } else if (innerValue instanceof List) {
                        mapValue.put(innerKey, processVariablesList((List<?>) innerValue));
                    } else {
                        processVariable(innerKey, innerValue, mapValue);
                    }
                });
    }

    private static List<?> processVariablesList(List<?> list) {
        return list.stream()
                .map(
                        variable -> {
                            if (variable instanceof String) {
                                String variableString = (String) variable;
                                return extractPlaceholder(variableString).stream()
                                        .reduce(
                                                variableString,
                                                (result, placeholder) -> {
                                                    return replacePlaceholders(
                                                            result,
                                                            placeholder,
                                                            System.getProperty(placeholder),
                                                            null);
                                                });
                            } else if (variable instanceof Map) {
                                processVariablesMap((Map<String, Object>) variable);
                                return variable;
                            } else if (variable instanceof List) {
                                return processVariablesList((List<?>) variable);
                            }
                            return variable;
                        })
                .collect(Collectors.toList());
    }

    private static void processVariable(
            String variableKey, Object variableValue, Map<String, Object> parentMap) {
        if (Objects.isNull(variableValue)) {
            return;
        }
        String variableString = variableValue.toString();
        List<String> placeholders = extractPlaceholder(variableString);

        for (String placeholder : placeholders) {
            String replacedValue =
                    replacePlaceholders(
                            variableString, placeholder, System.getProperty(placeholder), null);
            variableString = replacedValue;
        }

        if (!placeholders.isEmpty()) {
            parentMap.put(variableKey, variableString);
        }
    }

    public static List<String> extractPlaceholder(String input) {
        Pattern pattern = Pattern.compile(PLACEHOLDER_REGEX);
        Matcher matcher = pattern.matcher(input);
        List<String> placeholders = new ArrayList<>();

        while (matcher.find()) {
            placeholders.add(matcher.group(1));
        }

        return placeholders;
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

    /**
     * Parses MetadataConfig from the default seatunnel.yaml file.
     *
     * <p>The default path is $SEATUNNEL_HOME/config/seatunnel.yaml. If the file doesn't exist or
     * metadata is not configured, returns a default MetadataConfig (disabled).
     *
     * @return MetadataConfig parsed from seatunnel.yaml, or default MetadataConfig if not found
     */
    public static MetadataConfig parseMetadataConfigFromSeatunnelYaml() {
        String seatunnelHome = getSeatunnelHome();
        if (seatunnelHome == null) {
            log.info("SEATUNNEL_HOME not set, metadata provider disabled");
            return new MetadataConfig();
        }

        Path yamlPath = Paths.get(seatunnelHome, "config", "seatunnel.yaml");
        if (!Files.exists(yamlPath)) {
            log.info("seatunnel.yaml not found at {}, metadata provider disabled", yamlPath);
            return new MetadataConfig();
        }

        try {
            return parseMetadataConfig(yamlPath);
        } catch (Exception e) {
            log.warn("Failed to parse seatunnel.yaml for metadata config: {}", e.getMessage());
            return new MetadataConfig();
        }
    }

    /**
     * Gets the SEATUNNEL_HOME from environment variables or system properties.
     *
     * @return the SEATUNNEL_HOME path, or null if not set
     */
    private static String getSeatunnelHome() {
        String home = System.getenv("SEATUNNEL_HOME");
        if (home == null) {
            home = System.getProperty("SEATUNNEL_HOME");
        }
        return home;
    }

    private static MetadataConfig parseMetadataConfig(Path yamlPath) throws Exception {
        MetadataConfig config = new MetadataConfig();
        Map<String, String> properties = new HashMap<>();
        int seatunnelIndent = -1;
        int engineIndent = -1;
        int metadataIndent = -1;
        int providerIndent = -1;
        String providerKind = MetadataOptions.KIND.defaultValue();

        for (String line : Files.readAllLines(yamlPath)) {
            String trimmedLine = stripComment(line).trim();
            if (trimmedLine.isEmpty()) {
                continue;
            }

            int indent = countLeadingSpaces(line);
            if (metadataIndent < 0 && seatunnelIndent >= 0 && indent <= seatunnelIndent) {
                engineIndent = -1;
            }
            if (metadataIndent < 0 && engineIndent >= 0 && indent <= engineIndent) {
                engineIndent = -1;
            }
            if (metadataIndent >= 0 && indent <= metadataIndent) {
                break;
            }

            if (metadataIndent < 0) {
                String section = parseSection(trimmedLine);
                if ("seatunnel".equals(section)) {
                    seatunnelIndent = indent;
                } else if (seatunnelIndent >= 0
                        && indent == seatunnelIndent + 2
                        && "engine".equals(section)) {
                    engineIndent = indent;
                } else if (engineIndent >= 0
                        && indent == engineIndent + 2
                        && "metadata".equals(section)) {
                    metadataIndent = indent;
                }
                continue;
            }

            if (indent == metadataIndent + 2) {
                providerIndent = -1;
                KeyValue keyValue = parseKeyValue(trimmedLine);
                if (keyValue == null) {
                    String section = parseSection(trimmedLine);
                    if (providerKind.equalsIgnoreCase(section)) {
                        providerIndent = indent;
                    }
                    continue;
                }

                if (MetadataOptions.ENABLED.key().equals(keyValue.key)) {
                    config.setEnabled(Boolean.parseBoolean(keyValue.value));
                } else if (MetadataOptions.KIND.key().equals(keyValue.key)) {
                    providerKind = keyValue.value;
                    config.setKind(providerKind);
                }
                continue;
            }

            if (providerIndent >= 0 && indent == providerIndent + 2) {
                KeyValue keyValue = parseKeyValue(trimmedLine);
                if (keyValue != null) {
                    properties.put(keyValue.key, keyValue.value);
                }
            }
        }

        config.setProperties(properties);

        return config;
    }

    private static String stripComment(String line) {
        int commentIndex = line.indexOf('#');
        if (commentIndex < 0) {
            return line;
        }
        return line.substring(0, commentIndex);
    }

    private static int countLeadingSpaces(String line) {
        int count = 0;
        while (count < line.length() && line.charAt(count) == ' ') {
            count++;
        }
        return count;
    }

    private static String parseSection(String line) {
        if (!line.endsWith(":")) {
            return null;
        }
        return line.substring(0, line.length() - 1).trim();
    }

    private static KeyValue parseKeyValue(String line) {
        int separatorIndex = line.indexOf(':');
        if (separatorIndex <= 0 || separatorIndex == line.length() - 1) {
            return null;
        }
        String key = line.substring(0, separatorIndex).trim();
        String value = unquote(line.substring(separatorIndex + 1).trim());
        return new KeyValue(key, value);
    }

    private static String unquote(String value) {
        if (value.length() >= 2
                && ((value.startsWith("\"") && value.endsWith("\""))
                        || (value.startsWith("'") && value.endsWith("'")))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static class KeyValue {
        private final String key;
        private final String value;

        private KeyValue(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}

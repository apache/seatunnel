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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import com.hazelcast.internal.util.StringUtil;
import scala.Tuple2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.engine.common.Constant.REST_SUBMIT_JOBS_PARAMS;

public class RestUtil {
    private RestUtil() {}

    /**
     * Finds HOCON-style substitutions so REST env resolution can protect SeaTunnel placeholders.
     */
    private static final Pattern HOCON_PLACEHOLDER_PATTERN =
            Pattern.compile("\\$\\{([^}:]+)(:[^}]*)?}");

    /** Prefix used for temporary placeholder protection before Typesafe Config resolution. */
    private static final String PROTECTED_PLACEHOLDER_PREFIX =
            "__SEATUNNEL_REST_SYSTEM_PLACEHOLDER_";

    /** Suffix used for temporary placeholder protection before Typesafe Config resolution. */
    private static final String PROTECTED_PLACEHOLDER_SUFFIX = "__";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode convertByteToJsonNode(byte[] byteData) throws IOException {
        return objectMapper.readTree(byteData);
    }

    public static void buildRequestParams(Map<String, String> requestParams, String uri) {
        requestParams.put(RestConstant.JOB_ID, null);
        requestParams.put(RestConstant.IS_START_WITH_SAVE_POINT, String.valueOf(false));
        uri = StringUtil.stripTrailingSlash(uri);
        if (!uri.contains("?")) {
            return;
        }
        int indexEnd = uri.indexOf('?');
        try {
            for (String s : uri.substring(indexEnd + 1).split("&")) {
                String[] param = s.split("=");
                requestParams.put(param[0], URLDecoder.decode(param[1], "UTF-8"));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid Params format in Params.");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding exists in the parameter.");
        }
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
    }

    public static Config buildConfig(JsonNode jsonNode) {
        Map<String, Object> objectMap = JsonUtils.toMap(jsonNode);
        return ConfigBuilder.of(objectMap);
    }

    /**
     * Parses a HOCON REST request and resolves only explicitly allowed environment variables.
     *
     * <p>System properties and all other process environment variables are excluded because REST
     * callers must not be able to inspect arbitrary server-side secrets. Unresolved substitutions
     * remain unresolved, matching the CLI configuration loading policy.
     */
    public static Config buildHoconConfig(
            String content, Collection<String> environmentVariableAllowlist) {
        Map<String, Object> allowedEnvironment = new HashMap<>();
        if (environmentVariableAllowlist != null) {
            environmentVariableAllowlist.forEach(
                    variableName -> {
                        if (variableName != null && !variableName.isEmpty()) {
                            String value = System.getenv(variableName);
                            if (value != null) {
                                allowedEnvironment.put(variableName, value);
                            }
                        }
                    });
        }
        PlaceholderProtection placeholderProtection =
                protectSystemPlaceholders(content, allowedEnvironment.values());
        Config resolvedConfig =
                ConfigFactory.parseString(placeholderProtection.content)
                        .resolveWith(
                                ConfigFactory.parseMap(allowedEnvironment),
                                ConfigResolveOptions.noSystem().setAllowUnresolved(true));
        return restoreSystemPlaceholders(resolvedConfig, placeholderProtection.replacements);
    }

    /**
     * Temporarily protects SeaTunnel runtime placeholders from HOCON substitution resolution.
     *
     * <p>Those placeholders, for example {@code ${table_name}}, are consumed later by connector
     * runtime logic. They are not server-side environment variables and must not be resolved or
     * rejected by REST HOCON environment substitution.
     */
    private static PlaceholderProtection protectSystemPlaceholders(
            String content, Collection<Object> allowedEnvironmentValues) {
        Matcher matcher = HOCON_PLACEHOLDER_PATTERN.matcher(content);
        Map<String, String> replacements = new HashMap<>();
        StringBuffer protectedContent = new StringBuffer();
        int placeholderIndex = 0;
        String placeholderNonce = UUID.randomUUID().toString().replace("-", "");
        while (matcher.find()) {
            String placeholderName = matcher.group(1);
            if (!TablePlaceholder.isSystemPlaceholder(placeholderName)) {
                continue;
            }
            String replacement =
                    nextProtectedPlaceholderToken(
                            content,
                            allowedEnvironmentValues,
                            replacements.keySet(),
                            placeholderNonce,
                            placeholderIndex);
            placeholderIndex++;
            replacements.put(replacement, matcher.group());
            matcher.appendReplacement(protectedContent, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(protectedContent);
        return new PlaceholderProtection(protectedContent.toString(), replacements);
    }

    /**
     * Generates a temporary placeholder token that cannot collide with user config or env values.
     */
    private static String nextProtectedPlaceholderToken(
            String content,
            Collection<Object> allowedEnvironmentValues,
            Collection<String> usedTokens,
            String placeholderNonce,
            int placeholderIndex) {
        String replacement;
        int candidateIndex = placeholderIndex;
        do {
            replacement =
                    PROTECTED_PLACEHOLDER_PREFIX
                            + placeholderNonce
                            + "_"
                            + candidateIndex++
                            + PROTECTED_PLACEHOLDER_SUFFIX;
        } while (containsProtectedPlaceholderToken(
                content, allowedEnvironmentValues, usedTokens, replacement));
        return replacement;
    }

    /** Checks whether a generated placeholder token would collide with existing user data. */
    private static boolean containsProtectedPlaceholderToken(
            String content,
            Collection<Object> allowedEnvironmentValues,
            Collection<String> usedTokens,
            String replacement) {
        if (content.contains(replacement) || usedTokens.contains(replacement)) {
            return true;
        }
        return allowedEnvironmentValues.stream()
                .map(String::valueOf)
                .anyMatch(value -> value.contains(replacement));
    }

    /**
     * Restores protected SeaTunnel runtime placeholders after environment resolution completes.
     *
     * <p>If the config still contains an intentionally unresolved non-allowlisted substitution,
     * restoring through {@code root().unwrapped()} is impossible. In that failure path, returning
     * the unresolved config preserves the existing REST rejection behavior without exposing values.
     */
    private static Config restoreSystemPlaceholders(
            Config config, Map<String, String> protectedPlaceholders) {
        if (protectedPlaceholders.isEmpty()) {
            return config;
        }
        try {
            Map<String, Object> restoredConfig =
                    (Map<String, Object>)
                            restoreSystemPlaceholders(
                                    config.root().unwrapped(), protectedPlaceholders);
            return ConfigFactory.parseMap(restoredConfig)
                    .resolve(ConfigResolveOptions.noSystem().setAllowUnresolved(true));
        } catch (ConfigException.NotResolved ignored) {
            return config;
        }
    }

    /** Restores protected placeholders inside nested maps, lists and string values. */
    private static Object restoreSystemPlaceholders(
            Object value, Map<String, String> protectedPlaceholders) {
        if (value instanceof String) {
            String restoredValue = (String) value;
            for (Map.Entry<String, String> entry : protectedPlaceholders.entrySet()) {
                restoredValue = restoredValue.replace(entry.getKey(), entry.getValue());
            }
            return restoredValue;
        }
        if (value instanceof Map) {
            Map<String, Object> restoredMap = new LinkedHashMap<>();
            ((Map<?, ?>) value)
                    .forEach(
                            (mapKey, mapValue) ->
                                    restoredMap.put(
                                            String.valueOf(mapKey),
                                            restoreSystemPlaceholders(
                                                    mapValue, protectedPlaceholders)));
            return restoredMap;
        }
        if (value instanceof List) {
            List<Object> restoredList = new ArrayList<>();
            ((List<?>) value)
                    .forEach(
                            listValue ->
                                    restoredList.add(
                                            restoreSystemPlaceholders(
                                                    listValue, protectedPlaceholders)));
            return restoredList;
        }
        return value;
    }

    /** Holds HOCON content and the temporary placeholder replacement map. */
    private static class PlaceholderProtection {

        /** HOCON content after temporary placeholder protection. */
        private final String content;

        /** Mapping from temporary tokens to original SeaTunnel placeholders. */
        private final Map<String, String> replacements;

        private PlaceholderProtection(String content, Map<String, String> replacements) {
            this.content = content;
            this.replacements = replacements;
        }
    }

    public static List<Tuple2<Map<String, String>, Config>> buildConfigList(JsonNode jsonNode) {
        return StreamSupport.stream(jsonNode.spliterator(), false)
                .filter(JsonNode::isObject)
                .map(
                        node -> {
                            Map<String, Object> nodeMap = JsonUtils.toMap(node);
                            Map<String, String> params =
                                    (Map<String, String>) nodeMap.remove(REST_SUBMIT_JOBS_PARAMS);
                            Config config = ConfigBuilder.of(nodeMap);
                            return new Tuple2<>(params, config);
                        })
                .collect(Collectors.toList());
    }
}

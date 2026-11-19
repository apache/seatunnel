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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;

import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.command.ParameterSplitter;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import com.hazelcast.internal.util.StringUtil;
import scala.Tuple2;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.engine.common.Constant.REST_SUBMIT_JOBS_PARAMS;

public class RestUtil {
    private RestUtil() {}

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectReader JOB_SUBMISSION_READER =
            objectMapper.reader().with(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

    public static JsonNode convertByteToJsonNode(byte[] byteData) throws IOException {
        return objectMapper.readTree(byteData);
    }

    /** Reads a job submission before the tree can overwrite repeated JSON keys. */
    public static JsonNode readJobSubmissionJson(byte[] requestBody) {
        if (requestBody.length == 0) {
            throw new IllegalArgumentException("Request body is empty.");
        }
        try {
            return JOB_SUBMISSION_READER.readTree(requestBody);
        } catch (IOException e) {
            // The parser exception can include excerpts of a job config or variable values.
            throw new IllegalArgumentException("Invalid JSON format in request body.");
        }
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
                // params that only carried variables leave nothing after the '?'
                if (s.isEmpty()) {
                    continue;
                }
                int separator = s.indexOf('=');
                if (separator < 0) {
                    throw new IllegalArgumentException("Invalid Params format in Params.");
                }
                String key = s.substring(0, separator);
                if (RestConstant.VARIABLES.equals(decodeQueryComponent(key))) {
                    if (requestParams.containsKey(RestConstant.VARIABLES)) {
                        throw new IllegalArgumentException(
                                "Duplicate variables request parameter.");
                    }
                    requestParams.put(
                            RestConstant.VARIABLES,
                            decodeQueryComponent(s.substring(separator + 1)));
                    continue;
                }
                String[] param = s.split("=");
                requestParams.put(param[0], decodeQueryComponent(param[1]));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid Params format in Params.");
        }
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
    }

    private static String decodeQueryComponent(String component) {
        try {
            return URLDecoder.decode(component, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid URL encoding in query parameter.");
        }
    }

    public static Config buildConfig(JsonNode jsonNode) {
        Map<String, Object> objectMap = JsonUtils.toMap(jsonNode);
        return ConfigBuilder.of(objectMap);
    }

    /**
     * Builds a job config from a JSON request body. The variables come from the {@code variables}
     * request parameter and from an optional top-level {@code variables} object in the body, which
     * is not part of the job config.
     */
    public static Config buildConfig(JsonNode jsonNode, String variables) {
        Map<String, Object> objectMap = JsonUtils.toMap(jsonNode);
        Map<String, String> userVariables =
                parseVariables(variables, objectMap.remove(RestConstant.VARIABLES));
        return applyVariables(ConfigBuilder.of(objectMap), userVariables);
    }

    public static List<Tuple2<Map<String, String>, Config>> buildConfigList(JsonNode jsonNode) {
        return StreamSupport.stream(jsonNode.spliterator(), false)
                .filter(JsonNode::isObject)
                .map(
                        node -> {
                            Map<String, Object> nodeMap = JsonUtils.toMap(node);
                            Map<String, String> params =
                                    (Map<String, String>) nodeMap.remove(REST_SUBMIT_JOBS_PARAMS);
                            if (params == null) {
                                params = new HashMap<>();
                            }
                            Object variables =
                                    ((Map<String, ?>) params).remove(RestConstant.VARIABLES);
                            if (variables != null && !(variables instanceof String)) {
                                throw new IllegalArgumentException(
                                        "'variables' in 'params' must be a string");
                            }
                            Map<String, String> userVariables =
                                    parseVariables(
                                            (String) variables,
                                            nodeMap.remove(RestConstant.VARIABLES));
                            Config config =
                                    applyVariables(ConfigBuilder.of(nodeMap), userVariables);
                            return new Tuple2<>(params, config);
                        })
                .collect(Collectors.toList());
    }

    /**
     * Parses the {@code variables} request parameter, written like {@code -i} on the command line:
     * comma separated {@code key=value} pairs, where a value in double quotes may contain commas.
     * Unlike the command line, an entry without '=' is rejected rather than skipped.
     */
    public static Map<String, String> parseVariables(String variables) {
        return parseVariables(variables, null);
    }

    /**
     * Parses the {@code variables} request parameter together with a {@code variables} object from
     * a JSON request body. Error messages name the variable but never contain its value, which may
     * be a secret.
     */
    public static Map<String, String> parseVariables(String variables, Object bodyVariables) {
        Map<String, String> userVariables = new LinkedHashMap<>();
        Set<List<String>> variablePaths = new HashSet<>();
        if (variables != null) {
            List<String> entries = new ParameterSplitter().split(variables);
            for (int i = 0; i < entries.size(); i++) {
                if (entries.get(i).isEmpty()) {
                    continue;
                }
                String[] pair = entries.get(i).split("=", 2);
                if (pair.length != 2) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid entry %d in '%s': expected key=value",
                                    i + 1, RestConstant.VARIABLES));
                }
                putVariable(userVariables, variablePaths, pair[0].trim(), pair[1].trim(), i + 1);
            }
        }
        if (bodyVariables != null) {
            if (!(bodyVariables instanceof Map)) {
                throw new IllegalArgumentException(
                        "'"
                                + RestConstant.VARIABLES
                                + "' in the request body must be a JSON object");
            }
            int position = 0;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) bodyVariables).entrySet()) {
                String key = String.valueOf(entry.getKey()).trim();
                Object value = entry.getValue();
                if (!(value instanceof String
                        || value instanceof Number
                        || value instanceof Boolean)) {
                    throw new IllegalArgumentException(
                            "Invalid value for variable '"
                                    + key
                                    + "': expected a string, number or boolean");
                }
                putVariable(userVariables, variablePaths, key, value.toString().trim(), ++position);
            }
        }
        return userVariables;
    }

    /** Applies variables to a submitted job config, leaving the config untouched without any. */
    public static Config applyVariables(Config config, Map<String, String> variables) {
        return variables.isEmpty() ? config : ConfigBuilder.substituteVariables(config, variables);
    }

    /**
     * Adds one parsed request variable. Names must be nonempty Config paths and cannot name a
     * reserved table placeholder; repeated names are rejected instead of replacing an earlier
     * value. The position identifies malformed entries without including submitted values in
     * errors.
     */
    private static void putVariable(
            Map<String, String> variables,
            Set<List<String>> variablePaths,
            String key,
            String value,
            int position) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid entry %d in '%s': empty key",
                            position, RestConstant.VARIABLES));
        }
        List<String> path;
        try {
            path = ConfigUtil.splitPath(key);
        } catch (ConfigException e) {
            // an unquoted comma inside a value can turn part of the value into a key
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid entry %d in '%s': invalid variable name",
                            position, RestConstant.VARIABLES));
        }
        if (TablePlaceholder.isSystemPlaceholder(path.get(0))) {
            throw new IllegalArgumentException(
                    "Variable '" + key + "' is reserved for table placeholders");
        }
        if (!variablePaths.add(path)) {
            throw new IllegalArgumentException("Duplicate variable '" + key + "'");
        }
        // Keep the original spelling for substitution in quoted strings.
        variables.put(key, value);
    }
}

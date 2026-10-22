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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import com.hazelcast.internal.util.StringUtil;
import scala.Tuple2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.engine.common.Constant.REST_SUBMIT_JOBS_PARAMS;

public class RestUtil {
    private RestUtil() {}

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode convertByteToJsonNode(byte[] byteData) throws IOException {
        return objectMapper.readTree(byteData);
    }

    /**
     * Fills the request parameters of a v1 {@code /hazelcast/rest/maps/submit-job} request from the
     * query string of its URI. The values come from a URL, so they are URL-decoded, unlike the JSON
     * {@code params} values read by {@link #buildRequestParams(Map, Map)}, which must not be.
     *
     * @param requestParams the map to fill
     * @param uri the request URI, with or without a query string
     */
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
        checkStartWithSavePoint(requestParams);
    }

    /**
     * Fills the request parameters of one job of a {@code /submit-jobs} request from its {@code
     * params} object, with the same defaults and checks as {@link #buildRequestParams(Map,
     * String)}. The values are used as they are: they are not URL-encoded, so they are not decoded
     * either.
     *
     * @param requestParams the map to fill
     * @param params the {@code params} object of the job, may be {@code null} when it is omitted
     */
    public static void buildRequestParams(
            Map<String, String> requestParams, Map<String, String> params) {
        requestParams.put(RestConstant.JOB_ID, null);
        requestParams.put(RestConstant.IS_START_WITH_SAVE_POINT, String.valueOf(false));
        if (params != null) {
            requestParams.putAll(params);
        }
        checkStartWithSavePoint(requestParams);
    }

    private static void checkStartWithSavePoint(Map<String, String> requestParams) {
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
    }

    public static Config buildConfig(JsonNode jsonNode) {
        Map<String, Object> objectMap = JsonUtils.toMap(jsonNode);
        return ConfigBuilder.of(objectMap);
    }

    public static List<Tuple2<Map<String, String>, Config>> buildConfigList(JsonNode jsonNode) {
        return StreamSupport.stream(jsonNode.spliterator(), false)
                .filter(JsonNode::isObject)
                .map(
                        node -> {
                            Map<String, Object> nodeMap = JsonUtils.toMap(node);
                            Map<String, String> params =
                                    toStringParams(nodeMap.remove(REST_SUBMIT_JOBS_PARAMS));
                            Config config = ConfigBuilder.of(nodeMap);
                            return new Tuple2<>(params, config);
                        })
                .collect(Collectors.toList());
    }

    /**
     * Converts the optional {@code params} object of a {@code /submit-jobs} entry to string values,
     * so that a number such as {@code "jobId": 123} is accepted like {@code "jobId": "123"}.
     * Entries with a {@code null} value are skipped. A nested object or array value is rejected,
     * because it has no meaningful string form. The error messages name the key and the JSON type,
     * never the value, so that a large request value is not echoed back in the response.
     */
    private static Map<String, String> toStringParams(Object params) {
        Map<String, String> stringParams = new HashMap<>();
        if (params == null) {
            return stringParams;
        }
        if (!(params instanceof Map)) {
            throw new IllegalArgumentException(
                    "'"
                            + REST_SUBMIT_JOBS_PARAMS
                            + "' must be a JSON object, but was a JSON "
                            + jsonTypeOf(params));
        }
        ((Map<?, ?>) params)
                .forEach(
                        (key, value) -> {
                            if (value instanceof Map || value instanceof Collection) {
                                throw new IllegalArgumentException(
                                        "'"
                                                + REST_SUBMIT_JOBS_PARAMS
                                                + "."
                                                + key
                                                + "' must be a string, number or boolean,"
                                                + " but was a JSON "
                                                + jsonTypeOf(value));
                            }
                            if (value != null) {
                                stringParams.put(String.valueOf(key), String.valueOf(value));
                            }
                        });
        return stringParams;
    }

    /** Names the JSON type of a value produced by {@link JsonUtils#toMap(JsonNode)}. */
    private static String jsonTypeOf(Object value) {
        if (value instanceof Map) {
            return "object";
        }
        if (value instanceof Collection) {
            return "array";
        }
        if (value instanceof String) {
            return "string";
        }
        if (value instanceof Number) {
            return "number";
        }
        if (value instanceof Boolean) {
            return "boolean";
        }
        return "value";
    }
}

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

    public static Config buildConfig(JsonNode jsonNode, boolean isEncrypt) {
        Map<String, Object> objectMap = JsonUtils.toMap(jsonNode);
        return ConfigBuilder.of(objectMap, isEncrypt, true);
    }

    public static List<Tuple2<Map<String, String>, Config>> buildConfigList(
            JsonNode jsonNode, boolean isEncrypt) {
        return StreamSupport.stream(jsonNode.spliterator(), false)
                .filter(JsonNode::isObject)
                .map(
                        node -> {
                            Map<String, Object> nodeMap = JsonUtils.toMap(node);
                            Map<String, String> params =
                                    (Map<String, String>) nodeMap.remove(REST_SUBMIT_JOBS_PARAMS);
                            Config config = ConfigBuilder.of(nodeMap, isEncrypt, true);
                            return new Tuple2<>(params, config);
                        })
                .collect(Collectors.toList());
    }
}

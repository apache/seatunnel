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

package org.apache.seatunnel.connectors.seatunnel.posthog.source.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PostHogSourceParameterTest {

    @Test
    public void testBuildQueryRequest() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("base_url", "https://eu.posthog.com/");
        options.put("project_id", "project 1/test");
        options.put("api_key", "phx_test");
        options.put("query", "SELECT event, distinct_id FROM events LIMIT 10");
        options.put("retry", 3);
        options.put("connect_timeout_ms", 1000);
        options.put("socket_timeout_ms", 2000);
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Test", "value");
        headers.put("authorization", "ignored");
        headers.put("content-type", "text/plain");
        options.put("headers", headers);

        PostHogSourceParameter parameter = new PostHogSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(options));

        Assertions.assertEquals(
                "https://eu.posthog.com/api/projects/project%201%2Ftest/query/",
                parameter.getUrl());
        Assertions.assertEquals("post", parameter.getMethod().getMethod());
        Assertions.assertEquals("Bearer phx_test", parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals("value", parameter.getHeaders().get("X-Test"));
        Assertions.assertEquals("application/json", parameter.getHeaders().get("Accept"));
        Assertions.assertEquals("application/json", parameter.getHeaders().get("Content-Type"));
        Assertions.assertFalse(parameter.getHeaders().containsKey("authorization"));
        Assertions.assertFalse(parameter.getHeaders().containsKey("content-type"));
        Assertions.assertEquals(3, parameter.getRetry());
        Assertions.assertEquals(1000, parameter.getConnectTimeoutMs());
        Assertions.assertEquals(2000, parameter.getSocketTimeoutMs());

        JsonNode body = JsonUtils.stringToJsonNode(parameter.getBody());
        Assertions.assertEquals("HogQLQuery", body.at("/query/kind").asText());
        Assertions.assertEquals(
                "SELECT event, distinct_id FROM events LIMIT 10", body.at("/query/query").asText());
        Assertions.assertEquals("blocking", body.path("refresh").asText());
    }

    @Test
    public void testRejectBlankQuery() {
        Map<String, Object> options = new HashMap<>();
        options.put("project_id", "1");
        options.put("api_key", "phx_test");
        options.put("query", "  ");

        PostHogSourceParameter parameter = new PostHogSourceParameter();
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parameter.buildWithConfig(ReadonlyConfig.fromMap(options)));

        Assertions.assertTrue(exception.getMessage().contains("query"));
    }

    @Test
    public void testRejectInvalidBaseUrl() {
        Map<String, Object> options = new HashMap<>();
        options.put("base_url", "///");
        options.put("project_id", "1");
        options.put("api_key", "phx_test");
        options.put("query", "SELECT event FROM events");

        PostHogSourceParameter parameter = new PostHogSourceParameter();
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parameter.buildWithConfig(ReadonlyConfig.fromMap(options)));

        Assertions.assertTrue(exception.getMessage().contains("base_url"));
    }
}

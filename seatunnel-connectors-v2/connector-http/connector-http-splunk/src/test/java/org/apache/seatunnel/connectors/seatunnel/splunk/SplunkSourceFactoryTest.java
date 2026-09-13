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

package org.apache.seatunnel.connectors.seatunnel.splunk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class SplunkSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        SplunkSourceFactory factory = new SplunkSourceFactory();
        Assertions.assertEquals("Splunk", factory.factoryIdentifier());
    }

    @Test
    public void testHeadersInitializationWithoutExistingHeaders() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());
        String apiKey = "test-splunk-api-key";

        SplunkSourceParameter parameter = new SplunkSourceParameter();
        parameter.buildWithConfig(config, apiKey);

        Assertions.assertNotNull(parameter.getHeaders());
        Assertions.assertEquals(apiKey, parameter.getHeaders().get("Authorization"));
    }

    @Test
    public void testHeadersAndFormSemanticsInitialization() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());
        String apiKey = "Splunk test-splunk-api-key";

        SplunkSourceParameter parameter = new SplunkSourceParameter();
        parameter.buildWithConfig(config, apiKey);

        Assertions.assertNotNull(parameter.getHeaders());
        Assertions.assertEquals(apiKey, parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals(
                "application/x-www-form-urlencoded", parameter.getHeaders().get("Content-Type"));
        Assertions.assertTrue(parameter.isKeepParamsAsForm());
    }

    @Test
    public void testV2EndpointConfiguration() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("url", "https://your-splunk-instance:8089/services/search/v2/jobs/export");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        SplunkSourceParameter parameter = new SplunkSourceParameter();
        parameter.buildWithConfig(config, "Splunk test-key");

        Assertions.assertEquals(
                "https://your-splunk-instance:8089/services/search/v2/jobs/export",
                parameter.getUrl());
    }

    @Test
    public void testParamsDefaultOutputModeAndFormEncoding() {
        HashMap<String, Object> configMap = new HashMap<>();
        HashMap<String, Object> params = new HashMap<>();
        params.put("search", "search index=_internal | head 10");
        configMap.put("params", params);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        SplunkSourceParameter parameter = new SplunkSourceParameter();
        parameter.buildWithConfig(config, "Splunk test-key");

        Assertions.assertEquals("json", parameter.getParams().get("output_mode"));
        Assertions.assertEquals(
                "search index=_internal | head 10", parameter.getParams().get("search"));
        Assertions.assertTrue(parameter.isKeepParamsAsForm());
    }

    @Test
    public void testHttpClientPayloadCapture() throws Exception {
        try (okhttp3.mockwebserver.MockWebServer server =
                new okhttp3.mockwebserver.MockWebServer()) {
            server.enqueue(
                    new okhttp3.mockwebserver.MockResponse()
                            .setBody("{\"preview\": false, \"result\": {\"test\": \"data\"}}")
                            .setResponseCode(200));
            server.start();

            String baseUrl = server.url("/services/search/v2/jobs/export").toString();

            HashMap<String, Object> configMap = new HashMap<>();
            configMap.put("url", baseUrl);
            HashMap<String, Object> params = new HashMap<>();
            params.put("search", "search index=_internal | head 10");
            configMap.put("params", params);

            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
            SplunkSourceParameter parameter = new SplunkSourceParameter();
            parameter.buildWithConfig(config, "Splunk test-auth-token");

            okhttp3.mockwebserver.RecordedRequest recordedRequest = server.takeRequest();

            Assertions.assertEquals("POST", recordedRequest.getMethod());
            Assertions.assertEquals(
                    "application/x-www-form-urlencoded", recordedRequest.getHeader("Content-Type"));
            Assertions.assertEquals(
                    "Splunk test-auth-token", recordedRequest.getHeader("Authorization"));

            String requestBody = recordedRequest.getBody().readUtf8();
            Assertions.assertTrue(requestBody.contains("search="));
            Assertions.assertTrue(requestBody.contains("output_mode=json"));
        }
    }
}

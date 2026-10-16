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

package org.apache.seatunnel.connectors.seatunnel.intercom;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.intercom.config.IntercomSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.HashMap;
import java.util.Map;

public class IntercomSourceReaderTest {

    private IntercomSourceReader newReader(MockWebServer server, int retry) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("base_url", server.url("/").toString());
        configMap.put("api_key", "test-token");
        configMap.put("retry", retry);
        configMap.put("retry_backoff_multiplier_ms", 10);
        configMap.put("retry_backoff_max_ms", 50);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        IntercomSourceParameter parameter = new IntercomSourceParameter();
        parameter.buildWithConfig(config);
        return new IntercomSourceReader(parameter, null, null, null, null, null);
    }

    @Test
    public void testRetryOn429And5xx() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.enqueue(new MockResponse().setResponseCode(500));
            server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"data\":[]}"));
            server.start();
            IntercomSourceReader reader = newReader(server, 3);
            reader.open();
            try {
                Assertions.assertEquals(200, reader.executeRequest().getCode());
                Assertions.assertEquals(3, server.getRequestCount());
                RecordedRequest first = server.takeRequest();
                Assertions.assertEquals("Bearer test-token", first.getHeader("Authorization"));
                Assertions.assertTrue(first.getPath().startsWith("/contacts?"));
                Assertions.assertEquals("2.17", first.getHeader("Intercom-Version"));
                Assertions.assertTrue(first.getPath().contains("per_page=150"));
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testNoRetryOn401() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(401).setBody("{\"type\":\"error.list\"}"));
            server.start();
            IntercomSourceReader reader = newReader(server, 3);
            reader.open();
            try {
                Assertions.assertEquals(401, reader.executeRequest().getCode());
                Assertions.assertEquals(1, server.getRequestCount());
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testGivesUpAfterRetries() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            for (int i = 0; i < 4; i++) {
                server.enqueue(new MockResponse().setResponseCode(500));
            }
            server.start();
            IntercomSourceReader reader = newReader(server, 3);
            reader.open();
            try {
                RuntimeException ex =
                        Assertions.assertThrows(
                                RuntimeException.class, () -> reader.executeRequest());
                Assertions.assertTrue(ex.getMessage().contains("HTTP 500"));
                Assertions.assertEquals(4, server.getRequestCount());
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testInterruptedBackoffRestoresFlag() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.start();
            IntercomSourceReader reader = newReader(server, 3);
            reader.open();

            Thread.currentThread().interrupt();
            try {
                reader.executeRequest();
                Assertions.fail("Expected exception due to thread interruption");
            } catch (Exception e) {
                Assertions.assertTrue(
                        Thread.currentThread().isInterrupted(), "Interrupt flag must be restored");
                String msg = String.valueOf(e.getMessage());
                Assertions.assertFalse(
                        msg.contains("test-token"), "Message must not contain token");
                Assertions.assertFalse(
                        msg.contains("Authorization"), "Message must not contain Authorization");
            } finally {
                Thread.interrupted();
                reader.close();
            }
        }
    }

    @Test
    public void testCustomBackoffHonoredWithoutExplicitRetry() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-token");
        configMap.put("retry_backoff_multiplier_ms", 2000);
        configMap.put("retry_backoff_max_ms", 3000);
        IntercomSourceParameter parameter = new IntercomSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(3, parameter.getRetry());
        Assertions.assertEquals(2000, parameter.getRetryBackoffMultiplierMillis());
        Assertions.assertEquals(3000, parameter.getRetryBackoffMaxMillis());
    }

    @Test
    public void testDefaultBackoffWhenNothingConfigured() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-token");
        IntercomSourceParameter parameter = new IntercomSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(3, parameter.getRetry());
        Assertions.assertEquals(100, parameter.getRetryBackoffMultiplierMillis());
        Assertions.assertEquals(10000, parameter.getRetryBackoffMaxMillis());
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.asana;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.asana.config.AsanaSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AsanaSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        AsanaSourceFactory factory = new AsanaSourceFactory();
        Assertions.assertEquals("Asana", factory.factoryIdentifier());
    }

    @Test
    public void testParameterBuildingAndAuthHeader() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-asana-token-123");
        configMap.put("project_gid", "1234567890");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        AsanaSourceParameter parameter = new AsanaSourceParameter();
        parameter.buildWithConfig(config);

        Assertions.assertNotNull(parameter.getHeaders());
        Assertions.assertEquals(
                "Bearer test-asana-token-123", parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals(HttpRequestMethod.GET, parameter.getMethod());
        Assertions.assertEquals("1234567890", parameter.getParams().get("project"));
        Assertions.assertNotNull(parameter.getParams().get("opt_fields"));
    }

    @Test
    public void testModifiedSinceFilterOption() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-asana-token-123");
        configMap.put("project_gid", "1234567890");
        configMap.put("modified_since", "2026-01-01T00:00:00.000Z");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        AsanaSourceParameter parameter = new AsanaSourceParameter();
        parameter.buildWithConfig(config);

        Assertions.assertEquals(
                "2026-01-01T00:00:00.000Z", parameter.getParams().get("modified_since"));
    }

    @Test
    public void testBlankApiKeyValidation() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "   ");
        configMap.put("project_gid", "1234567890");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        AsanaSourceParameter parameter = new AsanaSourceParameter();
        Exception exception =
                Assertions.assertThrows(
                        Exception.class,
                        () -> {
                            parameter.buildWithConfig(config);
                        });

        String msg = exception.getMessage();
        Assertions.assertTrue(msg.contains("api_key"), "Should specify which option failed");
        Assertions.assertFalse(
                msg.contains("Authorization"), "Should not contain Authorization keyword");
    }

    @Test
    public void testBlankProjectGidValidation() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-asana-token-123");
        configMap.put("project_gid", "");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        AsanaSourceParameter parameter = new AsanaSourceParameter();
        Exception exception =
                Assertions.assertThrows(
                        Exception.class,
                        () -> {
                            parameter.buildWithConfig(config);
                        });

        String msg = exception.getMessage();
        Assertions.assertTrue(msg.contains("project_gid"), "Should specify which option failed");
        Assertions.assertFalse(
                msg.contains("test-asana-token-123"), "Should never leak the secret token");
    }

    @Test
    public void testSourceReaderPaginationAndNullAssignee() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            String page1 =
                    "{"
                            + "\"data\": ["
                            + "  {\"gid\": \"1\", \"name\": \"Task 1\", \"completed\": false, \"assignee\": {\"gid\": \"a1\", \"name\": \"Saloni\"}},"
                            + "  {\"gid\": \"2\", \"name\": \"Task 2\", \"completed\": true, \"assignee\": null}"
                            + "],"
                            + "\"next_page\": {\"offset\": \"ab.c_-==%2B\"}"
                            + "}";

            String page2 =
                    "{"
                            + "\"data\": ["
                            + "  {\"gid\": \"3\", \"name\": \"Task 3\", \"completed\": false, \"assignee\": {\"gid\": \"a2\", \"name\": \"Suhani\"}}"
                            + "],"
                            + "\"next_page\": null"
                            + "}";

            server.enqueue(new MockResponse().setBody(page1).setResponseCode(200));
            server.enqueue(new MockResponse().setBody(page2).setResponseCode(200));
            server.start();

            Map<String, Object> configMap = new HashMap<>();
            configMap.put("base_url", server.url("/").toString());
            configMap.put("api_key", "test-token");
            configMap.put("project_gid", "12345");
            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

            AsanaSource source = new AsanaSource(config);

            SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
            AsanaSourceReader reader =
                    (AsanaSourceReader) source.createReader(new SingleSplitReaderContext(ctx));

            List<SeaTunnelRow> emittedRows = new ArrayList<>();
            Collector<SeaTunnelRow> collector =
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow record) {
                            emittedRows.add(record);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return this;
                        }
                    };

            reader.open();
            try {
                reader.pollNext(collector);
            } finally {
                reader.close();
            }

            RecordedRequest first = server.takeRequest(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(first);
            Assertions.assertNull(first.getRequestUrl().queryParameter("offset"));

            RecordedRequest second = server.takeRequest(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(second);
            Assertions.assertEquals("ab.c_-==%2B", second.getRequestUrl().queryParameter("offset"));
            Assertions.assertEquals(2, server.getRequestCount());

            Assertions.assertEquals(3, emittedRows.size());
            Assertions.assertEquals("1", emittedRows.get(0).getField(0));
            Assertions.assertEquals("a1", emittedRows.get(0).getField(7));
            Assertions.assertEquals("2", emittedRows.get(1).getField(0));
            Assertions.assertNull(emittedRows.get(1).getField(7));
            Assertions.assertNull(emittedRows.get(1).getField(8));
            Assertions.assertEquals("3", emittedRows.get(2).getField(0));
        }
    }

    @Test
    public void testNegativeBackoffRejected() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-token");
        configMap.put("project_gid", "12345");
        configMap.put("retry_backoff_multiplier_ms", -1);
        AsanaSourceParameter parameter = new AsanaSourceParameter();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> parameter.buildWithConfig(ReadonlyConfig.fromMap(configMap)));
    }
}

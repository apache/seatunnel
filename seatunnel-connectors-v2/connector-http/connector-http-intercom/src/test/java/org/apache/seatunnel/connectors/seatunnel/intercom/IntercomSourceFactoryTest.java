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
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.intercom.config.IntercomSourceParameter;

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

public class IntercomSourceFactoryTest {

    private static Map<String, Object> baseConfig() {
        Map<String, Object> m = new HashMap<>();
        m.put("api_key", "test-intercom-token-123");
        return m;
    }

    @Test
    public void testFactoryIdentifier() {
        Assertions.assertEquals("Intercom", new IntercomSourceFactory().factoryIdentifier());
        Assertions.assertEquals(
                "Intercom",
                new IntercomSource(ReadonlyConfig.fromMap(baseConfig())).getPluginName());
    }

    @Test
    public void testParameterBuildingAndHeaders() {
        IntercomSourceParameter parameter = new IntercomSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals(
                "Bearer test-intercom-token-123", parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals("2.17", parameter.getHeaders().get("Intercom-Version"));
        Assertions.assertEquals(HttpRequestMethod.GET, parameter.getMethod());
        Assertions.assertEquals("https://api.intercom.io/contacts", parameter.getUrl());
        Assertions.assertEquals("150", parameter.getParams().get("per_page"));
        Assertions.assertFalse(parameter.getParams().containsKey("Authorization"));
    }

    @Test
    public void testRegionBaseUrlVersionAndPerPage() {
        Map<String, Object> m = baseConfig();
        m.put("base_url", "https://api.eu.intercom.io/");
        m.put("api_version", "2.14");
        m.put("per_page", 50);
        IntercomSourceParameter parameter = new IntercomSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(m));

        Assertions.assertEquals("https://api.eu.intercom.io/contacts", parameter.getUrl());
        Assertions.assertEquals("2.14", parameter.getHeaders().get("Intercom-Version"));
        Assertions.assertEquals("50", parameter.getParams().get("per_page"));
    }

    @Test
    public void testBlankApiKeyValidation() {
        Map<String, Object> m = baseConfig();
        m.put("api_key", "   ");
        Exception e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new IntercomSourceParameter()
                                        .buildWithConfig(ReadonlyConfig.fromMap(m)));
        Assertions.assertTrue(e.getMessage().contains("api_key"));
        Assertions.assertFalse(e.getMessage().contains("Authorization"));
    }

    @Test
    public void testPerPageOutOfRangeRejected() {
        for (int bad : new int[] {0, 151}) {
            Map<String, Object> m = baseConfig();
            m.put("per_page", bad);
            Exception e =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    new IntercomSourceParameter()
                                            .buildWithConfig(ReadonlyConfig.fromMap(m)));
            Assertions.assertTrue(e.getMessage().contains("per_page"));
            Assertions.assertFalse(e.getMessage().contains("test-intercom-token-123"));
        }
    }

    @Test
    public void testNegativeBackoffRejected() {
        Map<String, Object> m = baseConfig();
        m.put("retry_backoff_multiplier_ms", -1);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new IntercomSourceParameter().buildWithConfig(ReadonlyConfig.fromMap(m)));
    }

    @Test
    public void testSourceReaderPaginationAndNullFields() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            String page1 =
                    "{\"type\":\"list\",\"data\":["
                            + "{\"id\":\"c1\",\"role\":\"user\",\"email\":\"a@example.com\",\"name\":\"Alice\",\"phone\":null,\"external_id\":null,\"created_at\":1571672154,\"updated_at\":1571672200},"
                            + "{\"id\":\"c2\",\"role\":\"lead\",\"email\":\"b@example.com\",\"name\":null,\"phone\":\"+1123456789\",\"external_id\":\"ext-2\",\"created_at\":1571672155,\"updated_at\":1571672201}"
                            + "],\"pages\":{\"type\":\"pages\",\"next\":{\"page\":2,\"starting_after\":\"ab.c_-==%2B\"}}}";
            String page2 =
                    "{\"type\":\"list\",\"data\":["
                            + "{\"id\":\"c3\",\"role\":\"user\",\"email\":\"c@example.com\",\"name\":\"Carol\",\"created_at\":1571672156,\"updated_at\":1571672202}"
                            + "],\"pages\":{\"type\":\"pages\"}}";
            server.enqueue(new MockResponse().setBody(page1).setResponseCode(200));
            server.enqueue(new MockResponse().setBody(page2).setResponseCode(200));
            server.start();

            Map<String, Object> m = baseConfig();
            m.put("base_url", server.url("/").toString());
            IntercomSource source = new IntercomSource(ReadonlyConfig.fromMap(m));

            SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
            IntercomSourceReader reader =
                    (IntercomSourceReader) source.createReader(new SingleSplitReaderContext(ctx));

            List<SeaTunnelRow> rows = new ArrayList<>();
            Collector<SeaTunnelRow> collector =
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow record) {
                            rows.add(record);
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
            Assertions.assertNull(first.getRequestUrl().queryParameter("starting_after"));
            Assertions.assertEquals("150", first.getRequestUrl().queryParameter("per_page"));
            Assertions.assertEquals(
                    "Bearer test-intercom-token-123", first.getHeader("Authorization"));
            Assertions.assertEquals("2.17", first.getHeader("Intercom-Version"));

            RecordedRequest second = server.takeRequest(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(second);
            Assertions.assertEquals(
                    "ab.c_-==%2B", second.getRequestUrl().queryParameter("starting_after"));
            Assertions.assertEquals(2, server.getRequestCount());

            Assertions.assertEquals(3, rows.size());
            Assertions.assertEquals("c1", rows.get(0).getField(0));
            Assertions.assertEquals(1571672154L, rows.get(0).getField(6));
            Assertions.assertEquals(1571672200L, rows.get(0).getField(7));
            Assertions.assertNull(rows.get(0).getField(4));
            Assertions.assertNull(rows.get(0).getField(5));
            Assertions.assertNull(rows.get(1).getField(3));
            Assertions.assertEquals("ext-2", rows.get(1).getField(5));
            Assertions.assertEquals("c3", rows.get(2).getField(0));
            Assertions.assertNull(rows.get(2).getField(4));
        }
    }
}

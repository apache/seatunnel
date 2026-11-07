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

package org.apache.seatunnel.connectors.seatunnel.mixpanel;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MixpanelSourceFactoryTest {
    static Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("username", "fixture-user");
        options.put("password", "fixture-password");
        options.put("project_id", 42);
        options.put("from_date", "2026-01-01");
        options.put("to_date", "2026-01-02");
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("event", "string");
        fields.put("properties", "string");
        options.put("schema", java.util.Collections.singletonMap("fields", fields));
        return options;
    }

    @Test
    void discoversFactoryAndCreatesBoundedSource() {
        ClassLoader loader = getClass().getClassLoader();
        TableSourceFactory factory =
                FactoryUtil.discoverFactory(loader, TableSourceFactory.class, "Mixpanel");
        assertInstanceOf(MixpanelSourceFactory.class, factory);
        MixpanelSource source =
                (MixpanelSource)
                        factory.<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>createSource(
                                        new TableSourceFactoryContext(
                                                ReadonlyConfig.fromMap(options()), loader))
                                .createSource();
        assertEquals("Mixpanel", source.getPluginName());
        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        assertEquals(
                2, source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getTotalFields());
    }

    @Test
    void validatesRegionsAndDoesNotRequestLimit() {
        for (String region : new String[] {"US", "EU", "IN"}) {
            Map<String, Object> options = options();
            options.put("region", region);
            MixpanelSourceConfig config = new MixpanelSourceConfig(ReadonlyConfig.fromMap(options));
            assertEquals("https", config.endpoint.getScheme());
            assertEquals("/api/2.0/export", config.endpoint.getPath());
            assertEquals(
                    region.equals("US")
                            ? "data.mixpanel.com"
                            : "data-" + region.toLowerCase() + ".mixpanel.com",
                    config.endpoint.getHost());
        }
    }

    @Test
    void rejectsInvalidDatesCredentialsAndBoundsWithoutEchoingValues() {
        for (String key : new String[] {"username", "password", "from_date", "to_date"}) {
            rejects(key, " ");
        }
        for (String date :
                new String[] {"2026-02-29", "2026-1-01", "2026-01-01T00:00:00Z", "not-a-date"}) {
            rejects("from_date", date);
        }
        rejects("from_date", "2026-01-03");
        rejects("region", "https://attacker.invalid");
        rejects("username", "user:secret");
        rejects("password", "secret\r\nInjected: value");
        rejects("project_id", 0);
        for (String key :
                new String[] {
                    "connect_timeout_ms",
                    "read_timeout_ms",
                    "retry_backoff_ms",
                    "max_retry_backoff_ms",
                    "max_event_bytes"
                }) {
            rejects(key, 0);
            rejects(key, Integer.MAX_VALUE);
        }
        rejects("max_retries", -1);
        rejects("max_retries", 11);
        rejects("field_mapping", java.util.Collections.singletonMap("event", "$.event"));
        rejects("field_mapping", java.util.Collections.singletonMap("unknown", "/event"));
    }

    @Test
    void requiresTrustedOriginAndExplicitFixtureHttpOptIn() {
        for (String origin :
                new String[] {
                    "http://localhost:1234",
                    "https://user:fixture-password@example.com",
                    "https://example.com?key=fixture-password",
                    "https://example.com#fixture-password",
                    "https://example.com/other",
                    "file:///tmp/fixture-password",
                    "https://example.com:70000",
                    "https://example.com:0",
                    "not a URI"
                }) {
            rejects("base_url", origin);
        }
        Map<String, Object> options = options();
        options.put("base_url", "https://trusted.example:8443/");
        assertEquals(
                "https://trusted.example:8443/api/2.0/export",
                new MixpanelSourceConfig(ReadonlyConfig.fromMap(options)).endpoint.toString());
        options.put("base_url", "http://localhost:1234");
        options.put("allow_insecure_http", true);
        assertEquals(
                "http",
                new MixpanelSourceConfig(ReadonlyConfig.fromMap(options)).endpoint.getScheme());
    }

    @Test
    void rejectsMoreThanOneReader() {
        MixpanelSource source = new MixpanelSource(ReadonlyConfig.fromMap(options()));
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(1);
        assertThrows(IllegalArgumentException.class, () -> source.createReader(context));
    }

    @Test
    void rejectsStreamingModeAndInvertedBackoffBounds() {
        MixpanelSource source = new MixpanelSource(ReadonlyConfig.fromMap(options()));
        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH));
        assertThrows(
                IllegalArgumentException.class,
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING)));
        Map<String, Object> config = options();
        config.put("retry_backoff_ms", 6);
        config.put("max_retry_backoff_ms", 5);
        assertThrows(
                IllegalArgumentException.class,
                () -> new MixpanelSource(ReadonlyConfig.fromMap(config)));
    }

    private static void rejects(String key, Object value) {
        Map<String, Object> options = options();
        options.put(key, value);
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new MixpanelSource(ReadonlyConfig.fromMap(options)),
                        key);
        assertTrue(error.getMessage().contains("Mixpanel"), error.getMessage());
        assertFalse(error.toString().contains("fixture-password"));
        assertEquals(null, error.getCause());
    }
}

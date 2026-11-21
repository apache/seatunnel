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
package org.apache.seatunnel.connectors.seatunnel.amplitude;

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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AmplitudeSourceFactoryTest {
    static Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("api_key", "fixture-api-key");
        options.put("secret_key", "fixture-secret");
        options.put("start", "20260101T00");
        options.put("end", "20260101T01");
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("event_type", "string");
        fields.put("uuid", "string");
        fields.put("event_properties", "string");
        options.put("schema", Collections.singletonMap("fields", fields));
        return options;
    }

    @Test
    void discoversAmplitudeFactory() {
        ClassLoader loader = getClass().getClassLoader();
        TableSourceFactory factory =
                FactoryUtil.discoverFactory(loader, TableSourceFactory.class, "Amplitude");
        assertEquals("Amplitude", factory.factoryIdentifier());
        AmplitudeSource source =
                (AmplitudeSource)
                        factory.<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>createSource(
                                        new TableSourceFactoryContext(
                                                ReadonlyConfig.fromMap(options()), loader))
                                .createSource();
        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        assertEquals("Amplitude", source.getPluginName());
        assertEquals(
                3, source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getTotalFields());
    }

    @Test
    void validatesRegionalOrigins() {
        for (String region : new String[] {"US", "EU"}) {
            Map<String, Object> options = options();
            options.put("region", region);
            assertEquals(
                    "US".equals(region)
                            ? "https://amplitude.com/api/2/export"
                            : "https://analytics.eu.amplitude.com/api/2/export",
                    new AmplitudeSourceConfig(ReadonlyConfig.fromMap(options)).endpoint.toString());
        }
    }

    @Test
    void requiresElapsedInclusiveWindow() {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T04:00:00Z"), ZoneOffset.UTC);
        new AmplitudeSourceConfig(ReadonlyConfig.fromMap(options()), clock);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new AmplitudeSourceConfig(
                                ReadonlyConfig.fromMap(options()),
                                Clock.offset(clock, java.time.Duration.ofSeconds(-1))));
    }

    @Test
    void rejectsInvalidWindowsCredentialsAndBounds() {
        for (String key : new String[] {"api_key", "secret_key", "start", "end"}) {
            rejects(key, " ");
        }
        for (String value :
                new String[] {"20260229T00", "20260101T24", "2026-01-01", "20260101T00:00"}) {
            rejects("start", value);
        }
        rejects("start", "20260101T02");
        rejects("end", "20990101T00");
        rejects("start", "20240101T00");
        rejects("api_key", "user:secret");
        rejects("secret_key", "fixture-secret\r\nInjected: value");
        rejects("region", "other");
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
        rejects("max_archive_bytes", 0L);
        rejects("max_uncompressed_bytes", Long.MAX_VALUE);
        rejects("max_retries", -1);
        rejects("max_retries", 11);
        rejects("field_mapping", Collections.singletonMap("uuid", "$.uuid"));
        rejects("field_mapping", Collections.singletonMap("unknown", "/uuid"));
    }

    @Test
    void validatesBackoffAndTrustedOrigins() {
        for (String value :
                new String[] {
                    "http://localhost:1234",
                    "file:///tmp/fixture-secret",
                    "https://user:fixture-secret@example.com",
                    "https://example.com?fixture-secret",
                    "https://example.com/#fixture-secret",
                    "https://example.com/path",
                    "https://example.com:0",
                    "https://example.com:70000",
                    "not a URI"
                }) {
            rejects("base_url", value);
        }
        Map<String, Object> options = options();
        options.put("retry_backoff_ms", 6);
        options.put("max_retry_backoff_ms", 5);
        assertThrows(
                IllegalArgumentException.class,
                () -> new AmplitudeSource(ReadonlyConfig.fromMap(options)));
        Map<String, Object> fixture = options();
        fixture.put("base_url", "http://localhost:1234");
        fixture.put("allow_insecure_http", true);
        assertEquals(
                "http://localhost:1234/api/2/export",
                new AmplitudeSourceConfig(ReadonlyConfig.fromMap(fixture)).endpoint.toString());
    }

    @Test
    void rejectsStreamingAndMoreThanOneReader() {
        AmplitudeSource source = new AmplitudeSource(ReadonlyConfig.fromMap(options()));
        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH));
        assertThrows(
                IllegalArgumentException.class,
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING)));
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(1);
        assertThrows(IllegalArgumentException.class, () -> source.createReader(context));
    }

    private static void rejects(String key, Object value) {
        Map<String, Object> options = options();
        options.put(key, value);
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new AmplitudeSource(ReadonlyConfig.fromMap(options)));
        assertFalse(error.toString().contains("fixture-secret"));
    }
}

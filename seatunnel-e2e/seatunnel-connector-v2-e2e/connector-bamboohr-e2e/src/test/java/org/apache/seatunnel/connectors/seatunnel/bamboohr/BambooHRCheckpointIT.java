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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRConfig.Bounds;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSource.State;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic source SPI and calendar tests complement the real engine's HTTPS savepoint cases.
 */
class BambooHRCheckpointIT {
    private static final String KEY = "base64-must-not-be-in-source-state";
    private static final Instant NOW = Instant.parse("2024-03-10T23:30:00Z");

    @Test
    void limitsTimesheetsTo364DaysAndHandlesLeapMonthsAndTheConfiguredZone() {
        BambooHRConfig config =
                config(
                        "timesheet_entries",
                        Map.of(
                                "start_date",
                                "2022-01-01",
                                "company_time_zone",
                                "America/Los_Angeles"));
        Bounds clamped = config.freeze(NOW);
        assertEquals(LocalDate.parse("2023-03-12"), clamped.from);
        assertEquals(LocalDate.parse("2024-03-10"), clamped.until);
        assertEquals(
                LocalDate.parse("2024-02-29"),
                BambooHRReader.monthEnd(
                        LocalDate.parse("2024-02-20"), LocalDate.parse("2024-03-10")));

        BambooHRConfig exact =
                config(
                        "timesheet_entries",
                        Map.of(
                                "start_date",
                                "2024-03-01T00:15:00Z",
                                "end_date",
                                "2024-02-29",
                                "company_time_zone",
                                "America/Los_Angeles"));
        Bounds one = exact.freeze(NOW);
        assertEquals(LocalDate.parse("2024-02-29"), one.from);
        assertEquals(one.from, one.until);
    }

    @Test
    void computesEachOriginalWatermarkLookbackOnceAndPreservesTheExplicitFloor() {
        assertEquals(
                LocalDate.parse("2023-03-02"),
                config("time_off_requests_stream", Map.of("time_off_watermark", "2024-03-01"))
                        .freeze(NOW)
                        .from);
        assertEquals(
                LocalDate.parse("2024-01-31"),
                config("timesheet_entries", Map.of("timesheet_watermark", "2024-03-01"))
                        .freeze(NOW)
                        .from);
        assertEquals(
                LocalDate.parse("2024-02-02"),
                config(
                                "timesheet_entries",
                                Map.of(
                                        "timesheet_watermark",
                                        "2024-03-01",
                                        "start_date",
                                        "2024-02-02"))
                        .freeze(NOW)
                        .from);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        config(
                                        "timesheet_entries",
                                        Map.of(
                                                "start_date",
                                                "2022-01-01",
                                                "end_date",
                                                "2022-03-01"))
                                .freeze(NOW));
    }

    @Test
    void verifiesTheResumeOffsetOnlyAgainstTheSameWindowRowsAndOrder() {
        String first = "{\"id\":1,\"date\":\"2024-02-29\",\"hours\":3.50}";
        String second = "{\"id\":2,\"date\":\"2024-02-29\",\"hours\":null}";
        String digest = BambooHRJson.digestRows(List.of(first, second));
        assertDoesNotThrow(
                () ->
                        BambooHRReader.verifyReplay(
                                1,
                                digest,
                                BambooHRJson.digestRows(List.of(first, second)),
                                2,
                                "response rows"));
        IOException changed =
                assertThrows(
                        IOException.class,
                        () ->
                                BambooHRReader.verifyReplay(
                                        1,
                                        digest,
                                        BambooHRJson.digestRows(List.of(second, first)),
                                        2,
                                        "response rows"));
        assertTrue(changed.getMessage().contains("changed since its checkpoint"));
        assertThrows(
                IOException.class,
                () ->
                        BambooHRReader.verifyReplay(
                                1,
                                digest,
                                BambooHRJson.digestRows(List.of(first)),
                                1,
                                "response rows"));
        assertDoesNotThrow(
                () ->
                        BambooHRReader.verifyReplay(
                                0, null, BambooHRJson.digestRows(List.of()), 0, "response rows"));
    }

    @Test
    void restoresAnUnassignedSplitAndRejectsChangedCompanyOrSelectedFields() throws Exception {
        BambooHRConfig config = config("employees", Map.of("employee_fields", "firstName,1349"));
        BambooHRSource source = new BambooHRSource(config);
        AtomicInteger registered = new AtomicInteger();
        List<Cursor> assigned = new ArrayList<>();
        SourceSplitEnumerator.Context<Cursor> context = coordinator(registered, assigned);
        try (SourceSplitEnumerator<Cursor, State> initial = source.createEnumerator(context)) {
            initial.run();
            State saved = initial.snapshotState(1);
            assertEquals(1, initial.currentUnassignedSplitSize());
            assertTrue(assigned.isEmpty());

            BambooHRConfig otherFields =
                    config("employees", Map.of("employee_fields", "firstName"));
            BambooHRConfig otherCompany = config("employees", Map.of("company", "other"));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new BambooHRSource(otherFields).restoreEnumerator(context, saved));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new BambooHRSource(otherCompany).restoreEnumerator(context, saved));

            BambooHRConfig rotated =
                    config(
                            "employees",
                            Map.of(
                                    "employee_fields",
                                    "firstName,1349",
                                    "token",
                                    "safe-rotated-fixture-key"));
            try (SourceSplitEnumerator<Cursor, State> restored =
                    new BambooHRSource(rotated).restoreEnumerator(context, saved)) {
                restored.run();
                registered.set(1);
                restored.registerReader(0);
                restored.handleSplitRequest(0);
                assertEquals(1, assigned.size());
                assertSame(saved.pending.bounds, assigned.get(0).bounds);
                assertEquals(0, restored.currentUnassignedSplitSize());
            }
        }
    }

    @Test
    void terminalRestoreSignalsOnceAndCheckpointDoesNotContainCredentials() throws Exception {
        BambooHRConfig config = config("meta_fields_stream", Map.of());
        AtomicInteger ends = new AtomicInteger();
        SourceReader.Context context =
                (SourceReader.Context)
                        Proxy.newProxyInstance(
                                SourceReader.Context.class.getClassLoader(),
                                new Class<?>[] {SourceReader.Context.class},
                                (proxy, method, args) -> {
                                    if ("signalNoMoreElement".equals(method.getName())) {
                                        ends.incrementAndGet();
                                        return null;
                                    }
                                    throw new AssertionError("Unexpected reader context method");
                                });
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        throw new AssertionError("A terminal checkpoint must not emit rows");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        Cursor terminal = new Cursor(config.scope, config.freeze(NOW), null, 0, null, true);
        try (BambooHRReader reader = new BambooHRReader(config, context, "test")) {
            reader.open();
            assertTrue(reader.snapshotState(1).isEmpty());
            reader.addSplits(List.of(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, ends.get());
            assertTrue(reader.snapshotState(2).get(0).finished);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(terminal);
        }
        String stored = bytes.toString(StandardCharsets.ISO_8859_1);
        assertFalse(stored.contains(KEY));
        assertFalse(stored.contains("tenant.bamboohr.com"));
    }

    private static BambooHRConfig config(String resource, Map<String, Object> overrides) {
        Map<String, Object> options = new HashMap<>();
        options.put("company", "tenant");
        options.put("token", KEY);
        options.put("resource", resource);
        options.putAll(overrides);
        return new BambooHRConfig(ReadonlyConfig.fromMap(options));
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<Cursor> coordinator(
            AtomicInteger registered, List<Cursor> assigned) {
        return (SourceSplitEnumerator.Context<Cursor>)
                Proxy.newProxyInstance(
                        SourceSplitEnumerator.Context.class.getClassLoader(),
                        new Class<?>[] {SourceSplitEnumerator.Context.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "currentParallelism":
                                    return 1;
                                case "registeredReaders":
                                    return registered.get() == 0 ? Set.of() : Set.of(0);
                                case "assignSplit":
                                    assigned.add((Cursor) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError("Unexpected coordinator method");
                            }
                        });
    }
}

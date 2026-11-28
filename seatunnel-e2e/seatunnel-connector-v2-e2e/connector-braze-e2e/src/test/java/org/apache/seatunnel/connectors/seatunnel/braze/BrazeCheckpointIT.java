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

package org.apache.seatunnel.connectors.seatunnel.braze;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeSource.Split;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeSource.State;

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts source positions independently of the Zeta scheduling cadence.
 *
 * <p>The real engine cases cover transport and persisted recovery across pages and windows.
 */
class BrazeCheckpointIT {
    private static final String KEY = "synthetic-braze-key-excluded-from-state";
    private static final Instant NOW = Instant.parse("2024-01-16T02:03:04.567Z");
    private static final LocalDate FIRST = LocalDate.parse("2024-01-01");

    @Test
    void freezesThePartialCurrentDayAndUsesInclusiveWindowWidths() {
        BrazeConfig config = config("canvases_analytics", Map.of("start_date", FIRST.toString()));
        Instant cutoff = config.freeze(NOW);
        assertEquals(Instant.parse("2024-01-16T02:03:04Z"), cutoff);
        assertEquals(LocalDate.parse("2024-01-14"), config.lastDay(FIRST, cutoff));
        assertEquals(LocalDate.parse("2024-01-16"), config.lastDay(FIRST.plusDays(14), cutoff));
        assertEquals(
                "/canvas/data_series?canvas_id=parent&length=2&ending_at=2024-01-16T02%3A03%3A04Z"
                        + "&include_variant_breakdown=true&include_step_breakdown=true",
                config.recordTarget("parent", FIRST.plusDays(14), cutoff));
        assertThrows(
                IllegalArgumentException.class,
                () -> config("events_analytics", Map.of("start_date", "2024-01-17")).freeze(NOW));
    }

    @Test
    void detectsChangedOrderOrTruncatedRowsAtAPartlyEmittedPosition() {
        String first = BrazeJson.fingerprint(List.of("row1", "row2"));
        String changed = BrazeJson.fingerprint(List.of("row2", "row1"));
        assertDoesNotThrow(() -> BrazeReader.validateReplay(1, first, first, 2, "row page"));
        assertThrows(
                IOException.class,
                () -> BrazeReader.validateReplay(1, first, changed, 2, "row page"));
        assertThrows(
                IOException.class,
                () -> BrazeReader.validateReplay(1, first, first, 1, "row page"));
        assertThrows(
                IOException.class,
                () -> BrazeReader.validateReplay(0, first, changed, 2, "parent page"));
    }

    @Test
    void restoresBeforeAssignmentWithTheSameFrozenCutoff() throws Exception {
        Map<String, String> dates =
                Map.of("start_date", FIRST.toString(), "end_date", FIRST.toString());
        BrazeSource source = new BrazeSource(config("events_analytics", dates));
        AtomicInteger registered = new AtomicInteger();
        List<Split> assigned = new ArrayList<>();
        SourceSplitEnumerator.Context<Split> context = coordinator(registered, assigned);
        try (SourceSplitEnumerator<Split, State> first = source.createEnumerator(context)) {
            first.run();
            State saved = first.snapshotState(1);
            assertEquals(1, first.currentUnassignedSplitSize());
            assertTrue(assigned.isEmpty());
            try (SourceSplitEnumerator<Split, State> restored =
                    source.restoreEnumerator(context, saved)) {
                restored.run();
                registered.incrementAndGet();
                restored.registerReader(0);
                restored.handleSplitRequest(0);
                assertEquals(1, assigned.size());
                assertSame(saved.pending, assigned.get(0));
                assertEquals(0, restored.currentUnassignedSplitSize());
            }
            for (Map<String, String> changed :
                    List.of(
                            Map.of("token", "alternate-workspace"),
                            Map.of("instance", "eu-01"),
                            Map.of("end_date", "2024-01-02"))) {
                Map<String, String> overrides = new HashMap<>(dates);
                overrides.putAll(changed);
                BrazeSource other = new BrazeSource(config("events_analytics", overrides));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> other.restoreEnumerator(context, saved));
            }
        }
    }

    @Test
    void restoresTerminalWithoutSubmittingIoAndSerializesOnlyOffsetsAndFingerprints()
            throws Exception {
        BrazeConfig config = config("canvases_analytics", Map.of("start_date", FIRST.toString()));
        AtomicInteger eof = new AtomicInteger();
        SourceReader.Context context =
                (SourceReader.Context)
                        Proxy.newProxyInstance(
                                SourceReader.Context.class.getClassLoader(),
                                new Class<?>[] {SourceReader.Context.class},
                                (proxy, method, args) -> {
                                    if ("signalNoMoreElement".equals(method.getName())) {
                                        eof.incrementAndGet();
                                        return null;
                                    }
                                    throw new AssertionError("Unexpected reader context method");
                                });
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        throw new AssertionError("Terminal restore emitted a row");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        Instant cutoff = config.freeze(NOW);
        Split terminal = new Split(config.scope, cutoff, 0, null, null, null, 0, 0, null, true);
        try (BrazeReader reader = new BrazeReader(config, context, "canvases_analytics")) {
            reader.open();
            reader.pollNext(collector);
            assertEquals(0, eof.get());
            assertTrue(reader.snapshotState(1).isEmpty());
            reader.addSplits(List.of(terminal));
            reader.handleNoMoreSplits();
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, eof.get());
            assertTrue(reader.snapshotState(2).get(0).finished);
            assertNull(reader.snapshotState(2).get(0).from);
        }
        String privateId = "braze-private-parent-id";
        String privateRow = "braze-private-row";
        String ids = BrazeJson.fingerprint(List.of(privateId));
        String rows = BrazeJson.fingerprint(List.of(privateRow));
        Split partial = new Split(config.scope, cutoff, 0, null, ids, FIRST, 0, 1, rows, false);
        BrazeReader.validate(config, partial);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(partial);
        }
        String serialized = bytes.toString(StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains(KEY));
        assertFalse(serialized.contains(privateId));
        assertFalse(serialized.contains(privateRow));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        BrazeReader.validate(
                                config,
                                new Split(
                                        config.scope,
                                        cutoff,
                                        0,
                                        null,
                                        ids,
                                        FIRST.plusDays(1),
                                        0,
                                        1,
                                        rows,
                                        false)));
    }

    private static BrazeConfig config(String resource, Map<String, ?> overrides) {
        Map<String, Object> options = new HashMap<>();
        options.put("resource", resource);
        options.put("instance", "us-01");
        options.put("token", KEY);
        options.putAll(overrides);
        return new BrazeConfig(ReadonlyConfig.fromMap(options));
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<Split> coordinator(
            AtomicInteger registered, List<Split> assigned) {
        return (SourceSplitEnumerator.Context<Split>)
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
                                    assigned.add((Split) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError("Unexpected coordinator method");
                            }
                        });
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.Cursor;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.DocumentState;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.Page;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSource.Split;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSource.State;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
 * Uses deterministic source SPI, pagination and replay checks alongside the real Zeta cases.
 *
 * <p>Partial offsets and terminal state are independent of the engine's scheduling cadence.
 */
class BoldSignCheckpointIT {
    private static final String KEY = "synthetic-key-excluded-from-state";
    private static final Instant NOW = Instant.parse("2023-11-14T22:13:22Z");

    @Test
    void computesInclusiveWholeSecondsAndFreezesTheDefaultEnd() {
        BoldSignConfig config =
                config("documents", Map.of("start_date", "2023-11-14T23:13:20+01:00"));
        Window frozen = config.freeze(NOW);
        assertEquals(1700000000L, frozen.from.longValue());
        assertEquals(1700000002L, frozen.until.longValue());
        assertThrows(
                IllegalArgumentException.class,
                () -> config("documents", Map.of("start_date", "2023-11-14T22:13:20.123Z")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        config("documents", Map.of("start_date", "2023-11-14T22:13:23Z"))
                                .freeze(NOW));
    }

    @Test
    void rolloverRetainsUnseenEqualCursorRowsAndRejectsMissingOrRepeatedProgress()
            throws Exception {
        BoldSignConfig config = config("documents", Map.of("page_size", 100));
        Cursor position = new Cursor(100, 99, DocumentState.initial());
        String a = doc("preceding", 9007199254740994L, "cursor");
        String b = doc("overlap-1", 9007199254740993L, "cursor");
        String c = doc("overlap-2", 9007199254740993L, "nextCursor");
        String d = doc("new-same-cursor", 9007199254740993L, "cursor");
        String e = doc("after", 9007199254740992L, "cursor");
        Page boundary = page(config, position, 10001, a, b, c);
        Cursor next = boundary.next;
        assertEquals(9007199254740994L, next.anchor().longValue());
        assertEquals(1, next.page);
        assertEquals(100, next.completed);
        assertEquals(2, next.documents.overlap.size());
        Page tail = page(config, next, 4, b, c, d, e);
        assertEquals(List.of(d, e), tail.output);
        assertNull(tail.next);
        IOException missing = assertThrows(IOException.class, () -> page(config, next, 2, b, e));
        assertTrue(missing.getMessage().contains("saved overlap"));
        IOException tied =
                assertThrows(IOException.class, () -> page(config, position, 10001, b, c, d));
        assertTrue(tied.getMessage().contains("no distinct safe rollover anchor"));
        DocumentState repeated =
                new DocumentState(
                        next.anchor(),
                        null,
                        null,
                        List.of(),
                        null,
                        List.of(),
                        List.of(next.anchor()));
        assertThrows(
                IOException.class,
                () -> page(config, new Cursor(100, 199, repeated), 10001, a, b, c));
    }

    @Test
    void rejectsOffsetsWhenTheReplayedPageOrParentInventoryChanges() {
        String same = BoldSignJson.fingerprint(List.of("{\"id\":1}", "{\"id\":2}"), "2");
        String changed = BoldSignJson.fingerprint(List.of("{\"id\":2}", "{\"id\":1}"), "2");
        assertDoesNotThrow(() -> BoldSignReader.verifyReplay(1, same, same, 2, "data page"));
        assertThrows(
                IOException.class,
                () -> BoldSignReader.verifyReplay(1, same, changed, 2, "data page"));
        assertThrows(
                IOException.class,
                () -> BoldSignReader.verifyReplay(1, same, same, 1, "data page"));
        assertThrows(
                IOException.class,
                () -> BoldSignReader.verifyReplay(0, same, changed, 2, "brand inventory"));
    }

    @Test
    void restoresTheUnassignedCoordinatorWithItsFrozenWindowAndRejectsScopeDrift()
            throws Exception {
        BoldSignConfig config = config("documents", Map.of("start_date", "2023-11-14T22:13:20Z"));
        BoldSignSource source = new BoldSignSource(config);
        AtomicInteger registered = new AtomicInteger();
        List<Split> assigned = new ArrayList<>();
        SourceSplitEnumerator.Context<Split> context = coordinator(registered, assigned);
        try (SourceSplitEnumerator<Split, State> initial = source.createEnumerator(context)) {
            initial.run();
            State checkpoint = initial.snapshotState(1);
            assertEquals(1, initial.currentUnassignedSplitSize());
            assertTrue(assigned.isEmpty());
            for (Map<String, String> change :
                    List.of(
                            Map.of("region", "eu"),
                            Map.of("token", "another-key"),
                            Map.of("start_date", "2023-11-14T22:13:21Z"))) {
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new BoldSignSource(config("documents", change))
                                        .restoreEnumerator(context, checkpoint));
            }
            try (SourceSplitEnumerator<Split, State> restored =
                    source.restoreEnumerator(context, checkpoint)) {
                restored.run();
                registered.set(1);
                restored.registerReader(0);
                restored.handleSplitRequest(0);
                assertEquals(1, assigned.size());
                assertSame(checkpoint.pending.window, assigned.get(0).window);
                assertEquals(0, restored.currentUnassignedSplitSize());
            }
        }
    }

    @Test
    void restoresTerminalWithoutRequestsAndSerializesOnlySafeState() throws Exception {
        BoldSignConfig config = config("documents", Map.of());
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
                        throw new AssertionError("Terminal restore cannot emit rows");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        Window window = config.freeze(NOW);
        Split terminal = new Split(config.scope, window, 0, null, null, 0, null, true);
        try (BoldSignReader reader = new BoldSignReader(config, context, "documents")) {
            reader.open();
            assertTrue(reader.snapshotState(1).isEmpty());
            reader.addSplits(List.of(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, eof.get());
            assertTrue(reader.snapshotState(2).get(0).finished);
        }
        String privateId = "provider-signing-id-must-be-hashed";
        String privateRow = doc(privateId, 44, "cursor");
        Cursor pending =
                page(
                                config("documents", Map.of("page_size", 100)),
                                new Cursor(100, 99, DocumentState.initial()),
                                10001,
                                doc("preceding", 45, "cursor"),
                                privateRow)
                        .next;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(new Split(config.scope, window, 0, null, pending, 0, null, false));
        }
        String stored = bytes.toString(StandardCharsets.ISO_8859_1);
        assertFalse(stored.contains(KEY));
        assertFalse(stored.contains(privateId));
    }

    private static String doc(String id, long cursor, String member) {
        return "{\"documentId\":\"" + id + "\",\"" + member + "\":" + cursor + "}";
    }

    private static Page page(BoldSignConfig config, Cursor cursor, int total, String... records)
            throws IOException {
        String input =
                "{\"pageDetails\":{\"page\":"
                        + cursor.page
                        + ",\"pageSize\":100,"
                        + "\"totalRecordsCount\":"
                        + total
                        + "},\"result\":["
                        + String.join(",", records)
                        + "]}";
        return BoldSignPagination.prepare(
                config,
                Resource.DOCUMENTS,
                config.freeze(NOW),
                null,
                cursor,
                BoldSignJson.page(
                        BoldSignJson.parse(input.getBytes(StandardCharsets.UTF_8)),
                        Resource.DOCUMENTS,
                        cursor.page,
                        100,
                        10000));
    }

    private static BoldSignConfig config(String resource, Map<String, ?> overrides) {
        Map<String, Object> options = new HashMap<>();
        options.put("resource", resource);
        options.put("token", KEY);
        options.putAll(overrides);
        return new BoldSignConfig(ReadonlyConfig.fromMap(options));
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

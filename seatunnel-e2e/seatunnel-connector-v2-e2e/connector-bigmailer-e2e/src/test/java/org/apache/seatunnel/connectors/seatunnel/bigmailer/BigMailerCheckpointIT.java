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

package org.apache.seatunnel.connectors.seatunnel.bigmailer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerJson.Page;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerReader.Pager;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSource.PageCheckpoint;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSource.State;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
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
 * Verifies deterministic checkpoint boundaries that complement the real HTTPS Zeta savepoints.
 *
 * <p>Normal connector serialization and page verification are exercised without network fixtures.
 */
class BigMailerCheckpointIT {
    private static final String SECRET = "checkpoint-credential-value-must-not-be-stored";
    private static final String BRAND_A = "20000000-0000-4000-8000-000000000001";
    private static final String BRAND_B = "20000000-0000-4000-8000-000000000002";

    @Test
    void partialPageReplaysOnlyWithMatchingRowsOrderAndContinuation() throws Exception {
        String first = "{\"id\":\"91\",\"name\":\"精确\",\"value\":90071992547409931234567}";
        String second = "{\"id\":\"92\",\"value\":null}";
        Page before = page(true, "next", first, second);
        PageCheckpoint state = new PageCheckpoint(null, 0, 1, before.digest, List.of());
        Pager restored = new Pager(state);
        assertDoesNotThrow(() -> restored.accept(page(true, "next", first, second), 50));
        assertEquals(1, restored.snapshot().offset);
        for (Page changed :
                List.of(
                        page(true, "next", second, first),
                        page(true, "different", first, second),
                        page(true, "next", first))) {
            IOException exception =
                    assertThrows(IOException.class, () -> new Pager(state).accept(changed, 50));
            assertTrue(exception.getMessage().contains("changed since its checkpoint"));
        }
        assertEquals(before.rows.get(0).data, first);
    }

    @Test
    void separatesParentAndChildCursorHistoriesAndRejectsCorruptCheckpoint() throws Exception {
        String token = "same-token-from-different-parent";
        String hash = BigMailerJson.digestText(token);
        PageCheckpoint saved = new PageCheckpoint(token, 1, 0, null, List.of(hash));
        Pager parent = new Pager(saved);
        IOException error =
                assertThrows(IOException.class, () -> parent.accept(page(true, token), 10));
        assertTrue(error.getMessage().contains("repeated continuation cursor"));
        assertDoesNotThrow(() -> new Pager(PageCheckpoint.first()).accept(page(true, token), 10));
        assertEquals(List.of(hash), parent.snapshot().seen);

        BigMailerConfig child = config("contacts", Map.of());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        BigMailerReader.validate(
                                child,
                                new Cursor(child.scope, PageCheckpoint.first(), saved, false)));
        PageCheckpoint badActiveToken = new PageCheckpoint("other", 1, 0, null, List.of(hash));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        BigMailerReader.validate(
                                child,
                                new Cursor(
                                        child.scope,
                                        badActiveToken,
                                        PageCheckpoint.first(),
                                        false)));
    }

    @Test
    void parentReplayIgnoresNamesButRetainsIdsInOrderWithNextToken() throws Exception {
        Page parent = parentPage("more", BRAND_A, BRAND_B, "before");
        Pager restored = new Pager(new PageCheckpoint(null, 0, 0, parent.digest, List.of()));
        assertDoesNotThrow(
                () -> restored.accept(parentPage("more", BRAND_A, BRAND_B, "after"), 10));
        assertThrows(
                IOException.class,
                () -> restored.accept(parentPage("more", BRAND_B, BRAND_A, "before"), 10));
        assertThrows(
                IOException.class,
                () -> restored.accept(parentPage("other", BRAND_A, BRAND_B, "before"), 10));
        assertTrue(parent.rows.stream().allMatch(row -> row.data == null));
    }

    @Test
    void coordinatorRestoresInitialUnassignedTraversalAndRejectsScopeChanges() throws Exception {
        BigMailerConfig config = config("contacts", Map.of());
        BigMailerSource source = new BigMailerSource(config);
        AtomicInteger registered = new AtomicInteger();
        List<Cursor> assigned = new ArrayList<>();
        SourceSplitEnumerator.Context<Cursor> context = coordinator(registered, assigned);
        try (SourceSplitEnumerator<Cursor, State> initial = source.createEnumerator(context)) {
            initial.run();
            State state = initial.snapshotState(1);
            assertEquals(1, initial.currentUnassignedSplitSize());
            assertTrue(assigned.isEmpty());
            for (BigMailerConfig other :
                    List.of(
                            config("contacts", Map.of("token", "new-key")),
                            config("contacts", Map.of("page_size", 25)),
                            config("lists", Map.of()))) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigMailerSource(other).restoreEnumerator(context, state));
            }
            try (SourceSplitEnumerator<Cursor, State> restored =
                    source.restoreEnumerator(context, state)) {
                restored.run();
                registered.set(1);
                restored.registerReader(0);
                restored.handleSplitRequest(0);
                assertEquals(1, assigned.size());
                assertSame(state.pending, assigned.get(0));
                assertEquals(0, restored.currentUnassignedSplitSize());
            }
        }
    }

    @Test
    void terminalRestoreDoesNotFetchAndStateContainsNoApiKeyOrRecord() throws Exception {
        BigMailerConfig config = config("contacts", Map.of());
        Cursor terminal = new Cursor(config.scope, null, null, true);
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
                        throw new AssertionError("A terminal checkpoint cannot emit rows");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        try (BigMailerReader reader = new BigMailerReader(config, context, "contacts")) {
            reader.open();
            assertTrue(reader.snapshotState(1).isEmpty());
            reader.addSplits(List.of(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, ends.get());
            assertTrue(reader.snapshotState(2).get(0).finished);
        }
        String record = "{\"id\":1,\"email\":\"nominal@example.test\"}";
        Page snapshot = page(true, "more", record, "{\"id\":2}");
        Cursor active =
                new Cursor(
                        config.scope,
                        new PageCheckpoint(
                                null,
                                0,
                                0,
                                parentPage("parent", BRAND_A, BRAND_B, "test").digest,
                                List.of()),
                        new PageCheckpoint(null, 0, 1, snapshot.digest, List.of()),
                        false);
        BigMailerReader.validate(config, active);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(active);
        }
        String stored = bytes.toString(StandardCharsets.ISO_8859_1);
        assertFalse(stored.contains(SECRET));
        assertFalse(stored.contains("nominal@example.test"));
        assertFalse(stored.contains(BRAND_A));
    }

    private static Page page(boolean more, String cursor, String... records) throws IOException {
        String response =
                "{\"has_more\":"
                        + more
                        + ",\"cursor\":\""
                        + cursor
                        + "\",\"data\":["
                        + String.join(",", records)
                        + "]}";
        return BigMailerJson.page(
                BigMailerJson.parse(response.getBytes(StandardCharsets.UTF_8)),
                Resource.USERS,
                100,
                null,
                false);
    }

    private static Page parentPage(String token, String first, String second, String name)
            throws IOException {
        String response =
                "{\"has_more\":true,\"cursor\":\""
                        + token
                        + "\",\"data\":[{\"id\":\""
                        + first
                        + "\",\"name\":\""
                        + name
                        + "\"},{\"id\":\""
                        + second
                        + "\"}]}";
        return BigMailerJson.page(
                BigMailerJson.parse(response.getBytes(StandardCharsets.UTF_8)),
                Resource.BRANDS,
                100,
                null,
                true);
    }

    private static BigMailerConfig config(String resource, Map<String, Object> overrides) {
        Map<String, Object> values = new HashMap<>();
        values.put("resource", resource);
        values.put("token", SECRET);
        values.putAll(overrides);
        return new BigMailerConfig(ReadonlyConfig.fromMap(values));
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

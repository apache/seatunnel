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

package org.apache.seatunnel.connectors.seatunnel.blogger;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerConfig.Stage;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerJson.Page;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerReader.Pager;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSource.PageCheckpoint;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSource.State;

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
 * Deterministic pagination and source SPI checks complement the real Zeta HTTPS tests.
 *
 * <p>Exact in-page and terminal boundaries can be exercised without timing the engine scheduler.
 */
class BloggerCheckpointIT {
    private static final String REFRESH = "synthetic-blogger-refresh-value";
    private static final String CLIENT_SECRET = "synthetic-blogger-client-secret";
    private static final String BLOG = "765432123456770000000001";
    private static final String POST = "765432123456780000000002";

    @Test
    void partialPageRequiresSameRowsOrderAndContinuation() throws Exception {
        String first = "{\"id\":\"11\",\"content\":\"中文\",\"number\":90071992547409931234567}";
        String second = "{\"id\":\"22\",\"author\":null}";
        Page original = pages("continue", first, second);
        PageCheckpoint state = new PageCheckpoint(null, 0, 1, original.digest, List.of());
        Pager restored = new Pager(state);
        assertDoesNotThrow(() -> restored.accept(pages("continue", first, second), 10));
        assertEquals(1, restored.snapshot().offset);
        for (Page changed :
                List.of(
                        pages("continue", second, first),
                        pages("other", first, second),
                        pages("continue", first))) {
            IOException failure =
                    assertThrows(IOException.class, () -> new Pager(state).accept(changed, 10));
            assertTrue(failure.getMessage().contains("changed since its checkpoint"));
        }
        assertEquals(first, original.rows.get(0).json);
    }

    @Test
    void discoveryDigestsPreserveAncestryWithoutRetainingProviderRecords() throws Exception {
        Page first = discovery(POST, "before");
        Page harmless = discovery(POST, "after");
        Page changed = discovery("765432123456780000000009", "before");
        Pager restored = new Pager(new PageCheckpoint(null, 0, 0, first.digest, List.of()));
        assertDoesNotThrow(() -> restored.accept(harmless, 10));
        assertThrows(IOException.class, () -> restored.accept(changed, 10));
        assertEquals(BLOG, harmless.rows.get(0).blog);
        assertEquals(POST, harmless.rows.get(0).post);
        assertTrue(harmless.rows.stream().allMatch(row -> row.json == null));
    }

    @Test
    void rejectsOrphanChildProgressAndNonPaginatedCursorState() throws Exception {
        BloggerConfig config = config("comments", Map.of());
        String token = "next";
        String hash = BloggerJson.digest(token);
        PageCheckpoint start = PageCheckpoint.first();
        PageCheckpoint continued = new PageCheckpoint(token, 1, 0, null, List.of(hash));
        Cursor orphan = new Cursor(config.scope, List.of(start, start, continued), false);
        assertThrows(IllegalArgumentException.class, () -> BloggerReader.validate(config, orphan));
        Cursor badRoot = new Cursor(config.scope, List.of(continued, start, start), false);
        assertThrows(IllegalArgumentException.class, () -> BloggerReader.validate(config, badRoot));
        Pager pager = new Pager(continued);
        IOException cycle = assertThrows(IOException.class, () -> pager.accept(pages(token), 10));
        assertTrue(cycle.getMessage().contains("repeated continuation cursor"));
    }

    @Test
    void coordinatorRestoresThePendingTraversalAndRejectsNewCredentialScope() throws Exception {
        BloggerConfig config = config("comments", Map.of());
        BloggerSource source = new BloggerSource(config);
        AtomicInteger registered = new AtomicInteger();
        List<Cursor> assigned = new ArrayList<>();
        SourceSplitEnumerator.Context<Cursor> context = coordinator(registered, assigned);
        try (SourceSplitEnumerator<Cursor, State> initial = source.createEnumerator(context)) {
            initial.run();
            State state = initial.snapshotState(1);
            assertEquals(1, initial.currentUnassignedSplitSize());
            assertTrue(assigned.isEmpty());
            for (BloggerConfig changed :
                    List.of(
                            config("comments", Map.of("token", "another")),
                            config("pages", Map.of()),
                            config("comments", Map.of("page_size", 50)))) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BloggerSource(changed).restoreEnumerator(context, state));
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
    void terminalRestoreSignalsOnceAndCheckpointContainsNeitherOAuthNorPayload() throws Exception {
        BloggerConfig config = config("comments", Map.of());
        Cursor terminal = new Cursor(config.scope, List.of(), true);
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
                        throw new AssertionError("A terminal cursor cannot emit rows");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        try (BloggerReader reader = new BloggerReader(config, context, "comments")) {
            reader.open();
            assertTrue(reader.snapshotState(1).isEmpty());
            reader.addSplits(List.of(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, eof.get());
            assertTrue(reader.snapshotState(2).get(0).finished);
        }
        String fixed = BloggerJson.digest("synthetic-active-parent");
        Page data =
                pages(
                        "next",
                        "{\"id\":\"11\",\"email\":\"nominal@example.test\"}",
                        "{\"id\":\"22\"}");
        Cursor active =
                new Cursor(
                        config.scope,
                        List.of(
                                new PageCheckpoint(null, 0, 0, fixed, List.of()),
                                new PageCheckpoint(null, 0, 0, fixed, List.of()),
                                new PageCheckpoint(null, 0, 1, data.digest, List.of())),
                        false);
        BloggerReader.validate(config, active);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(active);
        }
        String saved = bytes.toString(StandardCharsets.ISO_8859_1);
        assertFalse(saved.contains(REFRESH));
        assertFalse(saved.contains(CLIENT_SECRET));
        assertFalse(saved.contains("nominal@example.test"));
    }

    private static Page pages(String next, String... items) throws IOException {
        String input =
                "{\"kind\":\"blogger#pageList\",\"nextPageToken\":\""
                        + next
                        + "\",\"items\":["
                        + String.join(",", items)
                        + "]}";
        return BloggerJson.page(
                BloggerJson.parse(input.getBytes(StandardCharsets.UTF_8)),
                Stage.PAGE_LIST,
                100,
                BLOG,
                null,
                false);
    }

    private static Page discovery(String post, String name) throws IOException {
        String input =
                "{\"kind\":\"blogger#postList\",\"items\":[{\"id\":\""
                        + post
                        + "\",\"title\":\""
                        + name
                        + "\",\"blog\":{\"id\":\""
                        + BLOG
                        + "\"}}]}";
        return BloggerJson.page(
                BloggerJson.parse(input.getBytes(StandardCharsets.UTF_8)),
                Stage.POST_LIST,
                100,
                BLOG,
                null,
                true);
    }

    private static BloggerConfig config(String resource, Map<String, Object> overrides) {
        Map<String, Object> values = new HashMap<>();
        values.put("resource", resource);
        values.put("client_id", "fixture.apps.googleusercontent.com");
        values.put("client_secret", CLIENT_SECRET);
        values.put("token", REFRESH);
        values.putAll(overrides);
        return new BloggerConfig(ReadonlyConfig.fromMap(values));
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

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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers frozen coordinator state, terminal restore and exact committed-prefix recovery behavior.
 */
class DatadogCheckpointTest {
    private static final Instant FROZEN_TIME = Instant.parse("2026-01-01T00:30:00Z");

    @Test
    void emptyAndTerminalReaderStateKeepsTheFrozenCoordinatorWindow() throws Exception {
        DatadogConfig config = config("logs");
        DatadogSplit fallback = config.initialSplit(FROZEN_TIME);
        EnumeratorContext enumeratorContext = new EnumeratorContext();
        DatadogSource.Enumerator enumerator =
                new DatadogSource.Enumerator(enumeratorContext, config, fallback);
        ReaderContext readerContext = new ReaderContext();

        try (DatadogReader reader =
                reader(config, readerContext, new AtomicInteger(), page(false))) {
            List<DatadogSplit> earliest = reader.snapshotState(1);
            assertEquals(Collections.emptyList(), earliest);
            enumerator.addSplitsBack(earliest, 0);
            enumerator.run();
            assertEquals(1, enumeratorContext.assigned.size());
            assertEquals(FROZEN_TIME, enumeratorContext.assigned.get(0).end);
        }

        DatadogSplit terminal = enumerator.snapshotState(2);
        terminal.complete = true;
        terminal.from = terminal.end;
        EnumeratorContext restoredContext = new EnumeratorContext();
        DatadogSource.Enumerator restored =
                new DatadogSource.Enumerator(restoredContext, config, enumerator.snapshotState(2));
        restored.addSplitsBack(Collections.singletonList(terminal), 0);
        restored.run();
        AtomicInteger requests = new AtomicInteger();
        try (DatadogReader reader = reader(config, readerContext, requests, page(false))) {
            reader.addSplits(restoredContext.assigned);
            reader.open();
            Output output = new Output();
            reader.pollNext(output);
            assertEquals(1, readerContext.finished);
            assertEquals(0, requests.get(), "Terminal restore must not reopen the provider");
            assertTrue(output.ids.isEmpty());
        }
    }

    @Test
    void midPageRestoreChecksThePrefixAndEmitsOnlyItsTail() throws Exception {
        DatadogConfig config = config("metrics");
        AtomicInteger requests = new AtomicInteger();
        DatadogSplit checkpoint;

        try (DatadogReader original = reader(config, new ReaderContext(), requests, page(false))) {
            original.open();
            original.addSplits(Collections.singletonList(config.initialSplit(FROZEN_TIME)));
            Output output = new Output();
            beforeDeadline(
                    () -> {
                        original.pollNext(output);
                        return !output.ids.isEmpty();
                    });
            checkpoint = original.snapshotState(1).get(0);
            assertEquals(64, checkpoint.prefix, "The checkpoint must bisect the returned page");
            assertEquals(ids(0, 64), output.ids);
        }

        ReaderContext restored = new ReaderContext();
        try (DatadogReader reader = reader(config, restored, requests, page(false))) {
            reader.open();
            reader.addSplits(Collections.singletonList(checkpoint));
            Output output = new Output();
            beforeDeadline(
                    () -> {
                        reader.pollNext(output);
                        return restored.finished == 1;
                    });
            assertEquals(ids(64, 130), output.ids);
            assertEquals(2, requests.get(), "Recovery must refetch the current page once");
            assertTrue(reader.snapshotState(2).get(0).complete);
        }

        try (DatadogReader reader = reader(config, new ReaderContext(), requests, page(true))) {
            reader.open();
            reader.addSplits(Collections.singletonList(checkpoint));
            Output output = new Output();
            DatadogResource.ProviderFailure error =
                    assertThrows(
                            DatadogResource.ProviderFailure.class,
                            () ->
                                    beforeDeadline(
                                            () -> {
                                                reader.pollNext(output);
                                                return !output.ids.isEmpty();
                                            }));
            assertTrue(error.getMessage().contains("checkpointed page prefix"));
            assertTrue(output.ids.isEmpty(), "Changed page must fail before emitting its tail");
        }
    }

    private static DatadogConfig config(String resource) {
        Map<String, Object> values = new HashMap<>();
        values.put("access_key", "fixture-access");
        values.put("secret_key", "fixture-secret");
        values.put("resource", resource);
        if (resource.equals("logs")) {
            values.put("start_time", "2026-01-01T00:00:00Z");
        }
        return new DatadogConfig(ReadonlyConfig.fromMap(values));
    }

    private static DatadogReader reader(
            DatadogConfig config,
            ReaderContext context,
            AtomicInteger requests,
            JsonNode response) {
        return new DatadogReader(
                config,
                context,
                () ->
                        new DatadogReader.PageClient() {
                            @Override
                            public JsonNode fetch(String target, long expires) {
                                requests.incrementAndGet();
                                return response;
                            }

                            @Override
                            public void close() {}
                        });
    }

    private static JsonNode page(boolean modified) {
        ObjectNode page = JsonNodeFactory.instance.objectNode();
        ArrayNode data = page.putArray("data");
        for (int i = 0; i < 130; i++) {
            data.addObject().put("id", "metric-" + i).put("value", modified && i == 0 ? -1 : i);
        }
        return page;
    }

    private static List<String> ids(int from, int to) {
        return IntStream.range(from, to).mapToObj(i -> "metric-" + i).collect(Collectors.toList());
    }

    private static void beforeDeadline(Attempt attempt) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
            if (attempt.await()) {
                return;
            }
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Reader did not progress");
    }

    /**
     * Bounded polling condition for observing asynchronous page preparation in component tests. The
     * enclosing deadline turns a stalled reader into a deterministic assertion failure.
     */
    private interface Attempt {
        boolean await() throws Exception;
    }

    /**
     * Captures emitted business IDs under the same collector lock used by checkpoint-aware polling.
     */
    private static final class Output implements Collector<SeaTunnelRow> {
        private final List<String> ids = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            ids.add((String) row.getField(1));
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }

    /**
     * Observes the source completion signal so terminal recovery cannot pass by merely opening. The
     * counter also detects duplicate completion signals.
     */
    private static final class ReaderContext implements SourceReader.Context {
        private int finished;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            finished++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    /**
     * Records actual split assignments to verify frozen fallback and restored-reader ownership. No
     * provider call is needed to observe coordinator dispatch.
     */
    private static final class EnumeratorContext
            implements SourceSplitEnumerator.Context<DatadogSplit> {
        private final List<DatadogSplit> assigned = new ArrayList<>();

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtaskId, List<DatadogSplit> splits) {
            assigned.addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtaskId) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}

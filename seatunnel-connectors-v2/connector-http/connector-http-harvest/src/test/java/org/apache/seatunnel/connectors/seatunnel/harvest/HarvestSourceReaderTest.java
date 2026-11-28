/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.harvest;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class HarvestSourceReaderTest {
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-11-24T00:00:00Z"), ZoneOffset.UTC);

    static String page(int page, String next, String entries) {
        return "{\"page\":"
                + page
                + ",\"next_page\":"
                + next
                + ",\"time_entries\":["
                + entries
                + "]}";
    }

    static String entry(long id) {
        return "{\"id\":" + id + ",\"notes\":null,\"hours\":1.2345,\"spent_date\":\"2026-11-23\"}";
    }

    private HarvestSourceReader reader(
            MockWebServer server,
            Map<String, Object> options,
            SourceReader.Context context,
            HarvestSourceReader.Sleeper sleeper) {
        options.put("base_url", server.url("/").toString());
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        return new HarvestSourceReader(
                new HarvestSourceConfig(config),
                CatalogTableUtil.buildWithConfig(config),
                new SingleSplitReaderContext(context),
                CLOCK,
                sleeper);
    }

    private static final class Rows implements Collector<SeaTunnelRow> {
        final List<SeaTunnelRow> values = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }

    @Test
    void readsPagesWithFiltersAndReplaysSnapshotFromFirstPage() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            String first = page(1, "2", entry(9007199254740993L));
            server.enqueue(new MockResponse().setBody(first));
            server.enqueue(new MockResponse().setBody(first));
            server.enqueue(new MockResponse().setBody(page(2, "null", entry(2))));
            Map<String, Object> options = HarvestSourceFactoryTest.options();
            options.put("from", "2026-11-01");
            options.put("to", "2026-11-24");
            options.put("updated_since", "2026-11-01T01:00:00+01:00");
            SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
            List<SingleSplit> state;
            try (HarvestSourceReader reader = reader(server, options, context, ignored -> {})) {
                reader.open();
                Rows original = new Rows();
                reader.pollNext(original);
                Assertions.assertEquals(1, original.values.size());
                state = reader.snapshotState(1);
                Assertions.assertArrayEquals(
                        new byte[] {1, 0},
                        state.get(0).getState(),
                        "no token or unstable page offset in state");
            }
            Rows restored = new Rows();
            try (HarvestSourceReader reader = reader(server, options, context, ignored -> {})) {
                reader.addSplits(state);
                reader.open();
                reader.pollNext(restored);
                reader.pollNext(restored);
                reader.pollNext(restored);
            }
            Assertions.assertEquals(2, restored.values.size());
            Assertions.assertEquals(9007199254740993L, restored.values.get(0).getField(0));
            Assertions.assertNull(restored.values.get(0).getField(1));
            Assertions.assertEquals(new BigDecimal("1.2345"), restored.values.get(0).getField(2));
            Assertions.assertEquals(LocalDate.of(2026, 11, 23), restored.values.get(0).getField(3));
            Mockito.verify(context).signalNoMoreElement();
            for (String page : new String[] {"1", "1", "2"}) {
                RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
                Assertions.assertNotNull(request);
                Assertions.assertEquals("GET", request.getMethod());
                Assertions.assertEquals("/v2/time_entries", request.getRequestUrl().encodedPath());
                Assertions.assertEquals(page, request.getRequestUrl().queryParameter("page"));
                Assertions.assertEquals("100", request.getRequestUrl().queryParameter("per_page"));
                Assertions.assertEquals(
                        "2026-11-01", request.getRequestUrl().queryParameter("from"));
                Assertions.assertEquals("2026-11-24", request.getRequestUrl().queryParameter("to"));
                Assertions.assertEquals(
                        "2026-11-01T01:00:00+01:00",
                        request.getRequestUrl().queryParameter("updated_since"));
                Assertions.assertEquals(
                        "Bearer " + HarvestSourceFactoryTest.TOKEN,
                        request.getHeader("Authorization"));
                Assertions.assertEquals("12345", request.getHeader("Harvest-Account-Id"));
                Assertions.assertFalse(request.getPath().contains(HarvestSourceFactoryTest.TOKEN));
            }
        }
    }

    @Test
    void completedCheckpointDoesNotReadAgain() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse().setBody(page(1, "null", entry(1))));
            List<SingleSplit> state;
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            ignored -> {})) {
                reader.open();
                reader.pollNext(new Rows());
                state = reader.snapshotState(2);
            }
            SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
            try (HarvestSourceReader reader =
                    reader(server, HarvestSourceFactoryTest.options(), context, ignored -> {})) {
                reader.addSplits(state);
                reader.open();
                Rows rows = new Rows();
                reader.pollNext(rows);
                Assertions.assertTrue(rows.values.isEmpty());
                Mockito.verify(context).signalNoMoreElement();
            }
            Assertions.assertEquals(1, server.getRequestCount());
        }
    }

    @Test
    void emptyIntermediatePageDoesNotFinishTheSplit() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(page(1, "2", "")));
            server.enqueue(new MockResponse().setBody(page(2, "null", entry(2))));
            server.start();
            SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
            try (HarvestSourceReader reader =
                    reader(server, HarvestSourceFactoryTest.options(), context, ignored -> {})) {
                reader.open();
                Rows rows = new Rows();
                reader.pollNext(rows);
                Assertions.assertTrue(rows.values.isEmpty());
                Mockito.verify(context, Mockito.never()).signalNoMoreElement();
                reader.pollNext(rows);
                Assertions.assertEquals(1, rows.values.size());
                Assertions.assertEquals(2L, rows.values.get(0).getField(0));
                Mockito.verify(context).signalNoMoreElement();
            }
            Assertions.assertEquals(2, server.getRequestCount());
        }
    }

    @Test
    void failedCollectionLeavesSplitUnfinishedForRecovery() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(page(1, "null", entry(1))));
            server.start();
            SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
            try (HarvestSourceReader reader =
                    reader(server, HarvestSourceFactoryTest.options(), context, ignored -> {})) {
                reader.open();
                Collector<SeaTunnelRow> output = Mockito.mock(Collector.class);
                Mockito.when(output.getCheckpointLock()).thenReturn(new Object());
                Mockito.doThrow(new IllegalStateException("sink rejected row"))
                        .when(output)
                        .collect(Mockito.any(SeaTunnelRow.class));
                Assertions.assertThrows(IllegalStateException.class, () -> reader.pollNext(output));
                Mockito.verify(context, Mockito.never()).signalNoMoreElement();
                Assertions.assertArrayEquals(
                        new byte[] {1, 0}, reader.snapshotState(1).get(0).getState());
            }
        }
    }

    @Test
    void emptyTerminalPageFinishesOnce() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(page(1, "null", "")));
            server.start();
            SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
            try (HarvestSourceReader reader =
                    reader(server, HarvestSourceFactoryTest.options(), context, ignored -> {})) {
                reader.open();
                Rows rows = new Rows();
                reader.pollNext(rows);
                reader.pollNext(rows);
                Assertions.assertTrue(rows.values.isEmpty());
                Mockito.verify(context).signalNoMoreElement();
                Assertions.assertEquals(1, server.getRequestCount());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{}",
                "[]",
                "not-json",
                "{\"page\":1,\"next_page\":null}",
                "{\"page\":1,\"next_page\":1,\"time_entries\":[{}]}",
                "{\"page\":1,\"next_page\":3,\"time_entries\":[{}]}",
                "{\"page\":2,\"next_page\":null,\"time_entries\":[]}",
                "{\"page\":1,\"next_page\":null,\"time_entries\":[1]}",
                "{\"page\":1,\"next_page\":null,\"time_entries\":[{\"id\":\"test-harvest-secret\"}]}",
                "{\"page\":1,\"next_page\":null,\"time_entries\":[{\"id\":1},{\"id\":\"test-harvest-secret\"}]}"
            })
    void rejectsMalformedPagesWithoutEmittingOrLeakingBody(String body) throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(body));
            server.start();
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            ignored -> {})) {
                reader.open();
                Rows rows = new Rows();
                IOException error =
                        Assertions.assertThrows(IOException.class, () -> reader.pollNext(rows));
                Assertions.assertTrue(rows.values.isEmpty());
                Assertions.assertFalse(error.toString().contains(HarvestSourceFactoryTest.TOKEN));
                Assertions.assertNull(error.getCause());
                Assertions.assertEquals(1, server.getRequestCount());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503, 504})
    void retriesOnlyRetryableResponsesOnSamePage(int status) throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(status).addHeader("Retry-After", "2"));
            server.enqueue(new MockResponse().setBody(page(1, "null", entry(1))));
            server.start();
            Rows rows = new Rows();
            List<Long> waits = new ArrayList<>();
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            delay -> {
                                Assertions.assertFalse(Thread.holdsLock(rows));
                                waits.add(delay);
                            })) {
                reader.open();
                reader.pollNext(rows);
            }
            Assertions.assertEquals(java.util.Collections.singletonList(2000L), waits);
            Assertions.assertEquals(1, rows.values.size());
            Assertions.assertEquals(server.takeRequest().getPath(), server.takeRequest().getPath());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 400, 401, 403, 404, 501})
    void doesNotRetryOrFollowRedirects(int status) throws Exception {
        try (MockWebServer server = new MockWebServer();
                MockWebServer other = new MockWebServer()) {
            other.start();
            server.enqueue(
                    new MockResponse()
                            .setResponseCode(status)
                            .addHeader("Location", other.url("/steal"))
                            .setBody(HarvestSourceFactoryTest.TOKEN));
            server.start();
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            ignored -> Assertions.fail("unexpected wait"))) {
                reader.open();
                IOException error =
                        Assertions.assertThrows(
                                IOException.class, () -> reader.pollNext(new Rows()));
                Assertions.assertTrue(error.getMessage().contains("HTTP " + status));
                Assertions.assertFalse(error.toString().contains(HarvestSourceFactoryTest.TOKEN));
            }
            Assertions.assertEquals(1, server.getRequestCount());
            Assertions.assertEquals(0, other.getRequestCount());
        }
    }

    @Test
    void honorsHttpDateAndUsesBackoffForMalformedHeader() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse()
                            .setResponseCode(429)
                            .addHeader("Retry-After", "Tue, 24 Nov 2026 00:00:03 GMT"));
            server.enqueue(
                    new MockResponse().setResponseCode(503).addHeader("Retry-After", "malformed"));
            server.enqueue(new MockResponse().setBody(page(1, "null", "")));
            server.start();
            List<Long> waits = new ArrayList<>();
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            waits::add)) {
                reader.open();
                reader.pollNext(new Rows());
            }
            Assertions.assertEquals(java.util.Arrays.asList(3000L, 2000L), waits);
        }
    }

    @Test
    void failsOnRetryExhaustionAndSupportsDisablingRetries() throws Exception {
        for (int retry : new int[] {0, 2}) {
            try (MockWebServer server = new MockWebServer()) {
                for (int i = 0; i <= retry; i++) {
                    server.enqueue(new MockResponse().setResponseCode(503));
                }
                server.start();
                Map<String, Object> options = HarvestSourceFactoryTest.options();
                options.put("retry", retry);
                try (HarvestSourceReader reader =
                        reader(
                                server,
                                options,
                                Mockito.mock(SourceReader.Context.class),
                                ignored -> {})) {
                    reader.open();
                    Assertions.assertThrows(IOException.class, () -> reader.pollNext(new Rows()));
                }
                Assertions.assertEquals(retry + 1, server.getRequestCount());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"61", "99999999999999999999999999"})
    void failsRatherThanRetryingBeforeProviderDelay(String delay) throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429).addHeader("Retry-After", delay));
            server.start();
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            ignored -> Assertions.fail("must not wait"))) {
                reader.open();
                Assertions.assertThrows(IOException.class, () -> reader.pollNext(new Rows()));
            }
            Assertions.assertEquals(1, server.getRequestCount());
        }
    }

    @Test
    void interruptionStopsRetryAndKeepsInterruptFlag() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.start();
            try (HarvestSourceReader reader =
                    reader(
                            server,
                            HarvestSourceFactoryTest.options(),
                            Mockito.mock(SourceReader.Context.class),
                            ignored -> {
                                throw new InterruptedException();
                            })) {
                reader.open();
                Assertions.assertThrows(
                        InterruptedException.class, () -> reader.pollNext(new Rows()));
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            Assertions.assertEquals(1, server.getRequestCount());
        }
    }
}

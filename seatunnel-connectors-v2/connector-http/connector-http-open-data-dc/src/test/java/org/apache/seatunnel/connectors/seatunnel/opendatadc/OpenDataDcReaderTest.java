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

package org.apache.seatunnel.connectors.seatunnel.opendatadc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpenDataDcReaderTest {
    @Test
    void scansShortPagesAndRestoresCursorWithoutFetchingAnotherUpperBound() throws Exception {
        RecordingContext context = new RecordingContext();
        final List<Long> requests = new ArrayList<>();
        final CrashesApi provider =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        requests.add(-1L);
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        requests.add(lastId);
                        assertEquals(19L, bound);
                        assertEquals(2, size);
                        return page(lastId == 0 ? 7 : 19);
                    }
                };
        OpenDataDcReader reader = new OpenDataDcReader(context, provider, 2, 0, "crashes");
        Rows rows = new Rows();
        reader.pollNext(rows);
        OpenDataDcReader restored = new OpenDataDcReader(context, provider, 2, 0, "crashes");
        restored.addSplits(reader.snapshotState(1));
        restored.pollNext(rows);
        restored.pollNext(rows);
        assertEquals(Arrays.asList(-1L, 0L, 7L), requests);
        assertEquals(2, rows.values.size());
        SeaTunnelRow first = rows.values.get(0);
        assertEquals(7L, first.getField(0));
        assertEquals("00123456", first.getField(1));
        assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0, 1), first.getField(2));
        assertEquals("Ward 1", first.getField(3));
        assertEquals(2, first.getField(4));
        assertEquals(38.9D, first.getField(5));
        assertEquals(null, first.getField(6));
        assertEquals(19L, rows.values.get(1).getField(0));
        assertEquals("crashes", first.getTableId());
        assertEquals(1, context.signals);
    }

    @Test
    void stopsAtCapAndShrinksLastRequest() throws IOException {
        RecordingContext context = new RecordingContext();
        final CrashesApi provider =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        assertEquals(lastId == 0L ? 2 : 1, size);
                        return page(lastId == 0 ? 7 : 12);
                    }
                };
        OpenDataDcReader reader = new OpenDataDcReader(context, provider, 2, 2, "crashes");
        Rows rows = new Rows();
        reader.pollNext(rows);
        reader.pollNext(rows);
        reader.pollNext(rows);
        assertEquals(2, rows.values.size());
        assertEquals(1, context.signals);
    }

    @Test
    void rejectsDuplicatePageBeforeEmittingAnyRows() throws IOException {
        CrashesApi provider =
                new CrashesApi() {
                    @Override
                    long readUpperBound() {
                        return 19;
                    }

                    @Override
                    JsonNode readPage(long lastId, long bound, int size) throws IOException {
                        return OpenDataDcReaderTest.parse(
                                "{\"features\":[" + feature(7) + "," + feature(7) + "]}");
                    }
                };
        OpenDataDcReader reader = new OpenDataDcReader(null, provider, 2, 0, "crashes");
        Rows rows = new Rows();
        assertThrows(IOException.class, () -> reader.pollNext(rows));
        assertEquals(0, rows.values.size());
    }

    @Test
    void rejectsArcGisErrorAndMalformedContract() {
        assertThrows(IOException.class, () -> parse("{\"error\":{\"code\":400},\"features\":[]}"));
        assertThrows(
                IOException.class,
                () -> parse("{\"features\":{},\"exceededTransferLimit\":false}"));
        assertThrows(
                IOException.class,
                () -> parse("{\"features\":[],\"exceededTransferLimit\":\"true\"}"));
    }

    @Test
    void honorsValidRetryAfterWithoutRetryingBeforeLongDeadlines() throws IOException {
        Instant now = Instant.parse("2026-11-21T08:00:00Z");
        assertEquals(0L, CrashesApi.retryDelayMillis("0", now, 500));
        assertEquals(2000L, CrashesApi.retryDelayMillis("2", now, 500));
        assertEquals(500L, CrashesApi.retryDelayMillis(null, now, 500));
        assertEquals(500L, CrashesApi.retryDelayMillis("bad", now, 500));
        assertEquals(2000L, CrashesApi.retryDelayMillis("Sat, 21 Nov 2026 08:00:02 GMT", now, 500));
        assertThrows(IOException.class, () -> CrashesApi.retryDelayMillis("6", now, 500));
        assertThrows(
                IOException.class,
                () -> CrashesApi.retryDelayMillis("999999999999999999999999999999", now, 500));
        assertThrows(
                IOException.class,
                () -> CrashesApi.retryDelayMillis("Sat, 21 Nov 2026 08:00:06 GMT", now, 500));
    }

    private static JsonNode page(long id) throws IOException {
        return parse("{\"features\":[" + feature(id) + "]}");
    }

    private static String feature(long id) {
        return "{\"attributes\":{\"OBJECTID\":"
                + id
                + ",\"CCN\":\"00123456\",\"REPORTDATE\":1000,"
                + "\"WARD\":\"Ward 1\",\"TOTAL_VEHICLES\":2,\"LATITUDE\":38.9,\"LONGITUDE\":null}}";
    }

    private static JsonNode parse(String json) throws IOException {
        return CrashesApi.parse(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    private static class RecordingContext extends SingleSplitReaderContext {
        private int signals;

        private RecordingContext() {
            super(null);
        }

        @Override
        public void signalNoMoreElement() {
            signals++;
        }
    }

    private static class Rows implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> values = new ArrayList<>();
        private final Object lock = new Object();

        @Override
        public void collect(SeaTunnelRow row) {
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}

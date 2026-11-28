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

package org.apache.seatunnel.connectors.seatunnel.apify;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetSource.State;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.message.BasicHttpResponse;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers deterministically placed checkpoints at source SPI boundaries. Real Zeta savepoints
 * separately exercise transitions between provider pages and between resources.
 */
class ApifyDatasetCheckpointIT {
    private static final String TOKEN = "synthetic-checkpoint-apify-token";
    private static final String DATASET = "actor-owner~snapshot-name";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void validatesPartiallyEmittedPageAfterTokenRotationAndRejectsChangedContent()
            throws Exception {
        ApifyDatasetConfig config = config(TOKEN, 4);
        ApifyPages.Page first = parse("[{\"a\":1,\"b\":2},{\"value\":\"next\"}]", 4, 2, 2, -1);
        Cursor partial = new Cursor(config.fingerprint, 0, 0, 0, first.end, 1, first.digest);
        byte[] bytes = serialize(partial);
        String checkpoint = new String(bytes, StandardCharsets.ISO_8859_1);
        assertFalse(checkpoint.contains(TOKEN));
        assertFalse(checkpoint.contains(DATASET));
        assertFalse(checkpoint.contains("next"));

        ApifyDatasetConfig rotated = config("rotated-synthetic-token", 4);
        assertEquals(config.fingerprint, rotated.fingerprint);
        Cursor restored = deserialize(bytes, Cursor.class);
        ApifyPages.Page reordered =
                parse("[{\"b\":2,\"a\":1},{\"value\":\"next\"}]", 2, 2, 5, restored.end);
        ApifyDatasetReader.verifyRestoredPage(restored, Resource.ITEM_COLLECTION, reordered);
        JsonNode value = JSON.readTree(reordered.rows.get(restored.rowOffset).payload);
        assertEquals("next", value.path("data").path("value").textValue());
        ApifyPages.Page changed =
                parse("[{\"a\":1,\"b\":2},{\"value\":\"changed\"}]", 2, 2, 5, restored.end);
        IOException rejected =
                assertThrows(
                        IOException.class,
                        () ->
                                ApifyDatasetReader.verifyRestoredPage(
                                        restored, Resource.ITEM_COLLECTION, changed));
        assertTrue(rejected.getMessage().contains("page changed since the checkpoint"));
    }

    @Test
    void coordinatorRestoresBeforeAssignmentAndRejectsChangedPageSize() throws Exception {
        ApifyDatasetSource source = new ApifyDatasetSource(config(TOKEN, 4));
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<Cursor> context = mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        State state;
        try (SourceSplitEnumerator<Cursor, State> before = source.createEnumerator(context)) {
            before.open();
            before.run();
            assertEquals(1, before.currentUnassignedSplitSize());
            state = deserialize(serialize(before.snapshotState(1)), State.class);
        }
        ApifyDatasetSource changed = new ApifyDatasetSource(config(TOKEN, 8));
        IllegalArgumentException rejected =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> changed.restoreEnumerator(context, state));
        assertTrue(rejected.getMessage().contains("query configuration changed during restore"));
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        try (SourceSplitEnumerator<Cursor, State> restored =
                source.restoreEnumerator(context, state)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            restored.handleSplitRequest(0);
            assertEquals(0, restored.currentUnassignedSplitSize());
            verify(context).assignSplit(eq(0), any(Cursor.class));
            verify(context).signalNoMoreSplits(0);
        }
    }

    @Test
    void terminalCursorEmitsOneEndSignalWithNoRowsOrHttpRequests() throws Exception {
        ApifyDatasetConfig config = config(TOKEN, 4);
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> sink =
                new Collector<SeaTunnelRow>() {
                    private final Object lock = new Object();

                    @Override
                    public void collect(SeaTunnelRow row) {
                        fail("terminal cursor must not collect a row");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return lock;
                    }
                };
        Cursor done = Cursor.start(config.fingerprint, 1);
        try (ApifyDatasetReader reader = new ApifyDatasetReader(config, context, "ApifyDataset")) {
            reader.open();
            assertTrue(reader.snapshotState(0).isEmpty());
            reader.addSplits(Collections.singletonList(deserialize(serialize(done), Cursor.class)));
            reader.pollNext(sink);
            reader.pollNext(sink);
            verify(context).signalNoMoreElement();
            assertEquals(1, reader.snapshotState(1).get(0).resourceIndex);
        }
    }

    private static ApifyPages.Page parse(String content, int limit, int count, int total, long end)
            throws IOException {
        HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
        response.setHeader("X-Apify-Pagination-Offset", "0");
        response.setHeader("X-Apify-Pagination-Limit", Integer.toString(limit));
        response.setHeader("X-Apify-Pagination-Count", Integer.toString(count));
        response.setHeader("X-Apify-Pagination-Total", Integer.toString(total));
        return ApifyPages.parse(
                Resource.ITEM_COLLECTION, response, JSON.readTree(content), DATASET, 0, limit, end);
    }

    private static ApifyDatasetConfig config(String token, int pageSize) {
        Map<String, Object> map = new HashMap<>();
        map.put("token", token);
        map.put("dataset_id", DATASET);
        map.put("resources", Collections.singletonList("item_collection"));
        map.put("page_size", pageSize);
        return new ApifyDatasetConfig(ReadonlyConfig.fromMap(map));
    }

    private static byte[] serialize(Object state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(state);
        }
        return bytes.toByteArray();
    }

    private static <T> T deserialize(byte[] bytes, Class<T> type) throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return type.cast(in.readObject());
        }
    }
}

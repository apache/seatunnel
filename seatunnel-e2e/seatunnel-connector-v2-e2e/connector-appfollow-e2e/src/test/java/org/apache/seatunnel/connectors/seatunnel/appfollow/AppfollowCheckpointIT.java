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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Batch;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Binding;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Request;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Resource;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowSource.State;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
 * Exercises precisely positioned AppFollow partial, unassigned and completed source checkpoints.
 * Real Zeta cases separately restore the parent graph and active metric child.
 */
class AppfollowCheckpointIT {
    private static final String ORIGINAL_TOKEN = "synthetic-state-appfollow-token";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void bindsPartialAppRowsAndDiscoveryToTheOriginalResponseWithoutPersistingTheToken()
            throws Exception {
        List<String> selected = Arrays.asList("app_lists", "ratings");
        AppfollowConfig config = config(ORIGINAL_TOKEN, selected);
        Request request = new Request(Resource.APP_LISTS, "8", null);
        String data =
                "{\"apps_app\":[{\"app\":{\"ext_id\":\"x\"},\"name\":\"payload-only\"},"
                        + "{\"app\":{\"ext_id\":\"y\"},\"app_id\":71}]}";
        Batch original = AppfollowData.parse(request, JSON.readTree(data), config);
        Cursor cursor =
                new Cursor(
                        config.fingerprint,
                        Resource.APP_LISTS.ordinal(),
                        1,
                        1,
                        original.digest,
                        Arrays.asList("7", "8"),
                        Collections.singletonList(new Binding("7", "x")));
        byte[] bytes = serialize(cursor);
        String serialized = new String(bytes, StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains(ORIGINAL_TOKEN));
        assertFalse(serialized.contains("payload-only"));

        AppfollowConfig rotated = config("rotated-state-token", selected);
        assertEquals(config.fingerprint, rotated.fingerprint);
        Cursor resume = deserialize(bytes, Cursor.class);
        assertEquals(Arrays.asList("7", "8"), resume.collections);
        assertEquals("7", resume.bindings.get(0).collection);
        assertEquals("x", resume.bindings.get(0).external);
        assertEquals(1, resume.parent);
        String reordered =
                data.replace(
                        "{\"app\":{\"ext_id\":\"y\"},\"app_id\":71}",
                        "{\"app_id\":71,\"app\":{\"ext_id\":\"y\"}}");
        Batch replay = AppfollowData.parse(request, JSON.readTree(reordered), rotated);
        AppfollowReader.verifyRestored(request, resume.row, resume.digest, replay);
        assertEquals("y", replay.rows.get(resume.row).external);
        Batch changed =
                AppfollowData.parse(
                        request, JSON.readTree(reordered.replace("\"y\"", "\"z\"")), rotated);
        IOException rejected =
                assertThrows(
                        IOException.class,
                        () ->
                                AppfollowReader.verifyRestored(
                                        request, resume.row, resume.digest, changed));
        assertTrue(rejected.getMessage().contains("response changed since the checkpoint"));
    }

    @Test
    void coordinatorRestoresItsUnassignedScanAndRejectsDifferentSelectedResources()
            throws Exception {
        AppfollowSource source =
                new AppfollowSource(config(ORIGINAL_TOKEN, Collections.singletonList("users")));
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<Cursor> context = mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        State state;
        try (SourceSplitEnumerator<Cursor, State> initial = source.createEnumerator(context)) {
            initial.open();
            initial.run();
            assertEquals(1, initial.currentUnassignedSplitSize());
            state = deserialize(serialize(initial.snapshotState(1)), State.class);
        }
        AppfollowSource changed =
                new AppfollowSource(config(ORIGINAL_TOKEN, Collections.singletonList("ratings")));
        assertThrows(
                IllegalArgumentException.class, () -> changed.restoreEnumerator(context, state));
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
    void terminalRestoreSignalsEofOnceAndMakesNoRequests() throws Exception {
        AppfollowConfig options = config(ORIGINAL_TOKEN, Collections.singletonList("users"));
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> sink =
                new Collector<SeaTunnelRow>() {
                    private final Object lock = new Object();

                    @Override
                    public void collect(SeaTunnelRow row) {
                        fail("completed scans cannot emit new rows");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return lock;
                    }
                };
        Cursor done =
                new Cursor(
                        options.fingerprint,
                        Resource.values().length,
                        0,
                        0,
                        null,
                        Collections.emptyList(),
                        Collections.emptyList());
        try (AppfollowReader reader = new AppfollowReader(options, context, "Appfollow")) {
            reader.open();
            assertTrue(reader.snapshotState(0).isEmpty());
            reader.addSplits(Collections.singletonList(deserialize(serialize(done), Cursor.class)));
            reader.pollNext(sink);
            reader.pollNext(sink);
            verify(context).signalNoMoreElement();
            assertEquals(Resource.values().length, reader.snapshotState(1).get(0).stage);
        }
    }

    private static AppfollowConfig config(String token, List<String> resources) {
        Map<String, Object> options = new HashMap<>();
        options.put("token", token);
        options.put("resources", resources);
        return new AppfollowConfig(ReadonlyConfig.fromMap(options));
    }

    private static byte[] serialize(Object value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static <T> T deserialize(byte[] bytes, Class<T> type) throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return type.cast(in.readObject());
        }
    }
}

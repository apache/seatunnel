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

package org.apache.seatunnel.connectors.seatunnel.appcues;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.appcues.AppcuesSource.Split;
import org.apache.seatunnel.connectors.seatunnel.appcues.AppcuesSource.State;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
 * Pins deterministic Appcues source state boundaries with no live provider. The real engine tests
 * separately exercise savepoints during HTTPS and across resources.
 */
class AppcuesCheckpointIT {
    private static final String USERNAME = "synthetic-state-key";
    private static final String PASSWORD = "synthetic-state-secret";
    private static final String START = "2024-01-01T00:00:00Z";
    private static final Instant END = Instant.parse("2024-01-03T00:00:00Z");
    private static final String CURSOR_TIME = "2024-01-02T01:00:00Z";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void canonicalPartialReplaySurvivesCredentialRotationAndRejectsChangedRows() throws Exception {
        AppcuesConfig config = config(USERNAME, PASSWORD, START, END.toString());
        String a = "{\"id\":\"a\",\"created_at\":\"" + CURSOR_TIME + "\",\"data\":\"unchanged\"}";
        String b = "{\"id\":\"b\",\"created_at\":\"" + CURSOR_TIME + "\",\"data\":{\"value\":5}}";
        AppcuesResource.Snapshot original = snapshot("[" + b + "," + a + "]", config);
        Split position = new Split(config.fingerprint, END, 0, 1, original.digest);
        byte[] checkpoint = serialize(position);
        String bytes = new String(checkpoint, StandardCharsets.ISO_8859_1);
        assertFalse(bytes.contains(USERNAME));
        assertFalse(bytes.contains(PASSWORD));
        assertFalse(bytes.contains(config.authorization.substring("Basic ".length())));
        assertFalse(bytes.contains("unchanged"));

        AppcuesConfig rotated = config("updated-key", "updated-secret", START, END.toString());
        assertEquals(config.fingerprint, rotated.fingerprint);
        Split restored = deserialize(checkpoint, Split.class);
        String reorderedB =
                "{\"data\":{\"value\":5},\"created_at\":\"" + CURSOR_TIME + "\",\"id\":\"b\"}";
        AppcuesResource.Snapshot reordered = snapshot("[" + a + "," + reorderedB + "]", rotated);
        AppcuesReader.verifyRestored(AppcuesResource.FLOWS, restored, reordered);
        assertEquals("b", reordered.rows.get(restored.offset).id);
        AppcuesResource.Snapshot changed =
                snapshot("[" + a + "," + b.replace("5", "6") + "]", rotated);
        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                AppcuesReader.verifyRestored(
                                        AppcuesResource.FLOWS, restored, changed));
        assertTrue(failure.getMessage().contains("filtered response changed since the checkpoint"));
    }

    @Test
    void restoresTheUnassignedWindowAndRejectsAChangedQuery() throws Exception {
        AppcuesSource source = new AppcuesSource(config(USERNAME, PASSWORD, START, null));
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<Split> coordinator =
                mock(SourceSplitEnumerator.Context.class);
        when(coordinator.currentParallelism()).thenReturn(1);
        when(coordinator.registeredReaders()).thenReturn(Collections.emptySet());
        State saved;
        try (SourceSplitEnumerator<Split, State> initial = source.createEnumerator(coordinator)) {
            initial.open();
            initial.run();
            assertEquals(1, initial.currentUnassignedSplitSize());
            saved = deserialize(serialize(initial.snapshotState(1)), State.class);
        }
        AppcuesSource changed =
                new AppcuesSource(config(USERNAME, PASSWORD, "2024-01-02T00:00:00Z", null));
        assertThrows(
                IllegalArgumentException.class,
                () -> changed.restoreEnumerator(coordinator, saved));

        when(coordinator.registeredReaders()).thenReturn(Collections.singleton(0));
        try (SourceSplitEnumerator<Split, State> restored =
                source.restoreEnumerator(coordinator, saved)) {
            restored.open();
            assertEquals(saved.end, restored.snapshotState(2).end);
            restored.run();
            restored.registerReader(0);
            restored.handleSplitRequest(0);
            assertEquals(0, restored.currentUnassignedSplitSize());
            verify(coordinator).assignSplit(eq(0), any(Split.class));
            verify(coordinator).signalNoMoreSplits(0);
        }
    }

    @Test
    void terminalRestoreSignalsEofExactlyOnce() throws Exception {
        AppcuesConfig config = config(USERNAME, PASSWORD, START, END.toString());
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    private final Object lock = new Object();

                    @Override
                    public void collect(SeaTunnelRow data) {
                        fail("a terminal restore cannot emit another row");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return lock;
                    }
                };
        Split complete = new Split(config.fingerprint, END, 1, 0, null);
        try (AppcuesReader reader = new AppcuesReader(config, context, "Appcues")) {
            reader.open();
            assertTrue(reader.snapshotState(0).isEmpty());
            reader.addSplits(
                    Collections.singletonList(deserialize(serialize(complete), Split.class)));
            reader.pollNext(collector);
            reader.pollNext(collector);
            verify(context).signalNoMoreElement();
            assertEquals(1, reader.snapshotState(1).get(0).resource);
        }
    }

    private static AppcuesResource.Snapshot snapshot(String value, AppcuesConfig config)
            throws IOException {
        return AppcuesResource.FLOWS.snapshot(
                JSON.readTree(value), config.start, END, config.maxRecords);
    }

    private static AppcuesConfig config(
            String username, String password, String start, String end) {
        Map<String, Object> values = new HashMap<>();
        values.put("username", username);
        values.put("password", password);
        values.put("account_id", "fixture-account");
        values.put("start_date", start);
        if (end != null) {
            values.put("end_date", end);
        }
        values.put("resources", Collections.singletonList("flows"));
        return new AppcuesConfig(ReadonlyConfig.fromMap(values));
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

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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

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
 * Pins source SPI boundaries that a running job cannot reliably pause on. The Zeta suite separately
 * verifies real checkpoints, savepoint restoration, output and request counts.
 */
class AlphaVantageCheckpointIT {
    private static final String TOKEN = "synthetic-checkpoint-key";
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Test
    void separatesCredentialRotationFromQueryFingerprintAndRecognizesChangedSnapshot()
            throws Exception {
        AlphaVantageConfig config = config(TOKEN);
        AlphaVantageResource resource = AlphaVantageResource.TIME_SERIES_DAILY;
        AlphaVantageResource.Snapshot original =
                resource.snapshot(
                        series(
                                "\"2026-11-06\":{\"4. close\":\"2.0000\",\"1. open\":\"1\"},"
                                        + "\"2026-11-03\":{\"4. close\":\"0.0001\"}"),
                        config);
        AlphaVantageSource.Split checkpoint =
                new AlphaVantageSource.Split(config.fingerprint, 0, 1, original.identity);
        byte[] serialized = serialize(checkpoint);
        assertFalse(new String(serialized, StandardCharsets.ISO_8859_1).contains(TOKEN));

        AlphaVantageConfig rotated = config("rotated-synthetic-key");
        assertEquals(config.fingerprint, rotated.fingerprint);
        AlphaVantageSource.Split restored = deserialize(serialized, AlphaVantageSource.Split.class);
        AlphaVantageResource.Snapshot reordered =
                resource.snapshot(
                        series(
                                "\"2026-11-03\":{\"4. close\":\"0.0001\"},"
                                        + "\"2026-11-06\":{\"1. open\":\"1\",\"4. close\":\"2.0000\"}"),
                        rotated);
        AlphaVantageReader.validateRestored(resource, restored, reordered);
        assertEquals("2026-11-06", reordered.points.get(restored.offset).date);

        AlphaVantageResource.Snapshot changed =
                resource.snapshot(
                        series(
                                "\"2026-11-03\":{\"4. close\":\"0.0001\"},"
                                        + "\"2026-11-06\":{\"1. open\":\"1\",\"4. close\":\"2.0001\"}"),
                        rotated);
        IOException rejected =
                assertThrows(
                        IOException.class,
                        () -> AlphaVantageReader.validateRestored(resource, restored, changed));
        assertTrue(rejected.getMessage().contains("response changed since the checkpoint"));
    }

    @Test
    void restoresAnEnumeratorSnapshotMadeBeforeAssignment() throws Exception {
        AlphaVantageSource source = new AlphaVantageSource(config(TOKEN));
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<AlphaVantageSource.Split> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        AlphaVantageSource.State state;
        try (SourceSplitEnumerator<AlphaVantageSource.Split, AlphaVantageSource.State> fresh =
                source.createEnumerator(context)) {
            fresh.open();
            fresh.run();
            state = deserialize(serialize(fresh.snapshotState(1)), AlphaVantageSource.State.class);
            assertEquals(1, fresh.currentUnassignedSplitSize());
        }
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        try (SourceSplitEnumerator<AlphaVantageSource.Split, AlphaVantageSource.State> restored =
                source.restoreEnumerator(context, state)) {
            restored.open();
            restored.registerReader(0);
            restored.run();
            restored.handleSplitRequest(0);
            assertEquals(0, restored.currentUnassignedSplitSize());
            verify(context).assignSplit(eq(0), any(AlphaVantageSource.Split.class));
            verify(context).signalNoMoreSplits(0);
        }
    }

    @Test
    void restoresACompletedSelectionWithOneEofAndNoRows() throws Exception {
        AlphaVantageConfig config = config(TOKEN);
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    private final Object checkpointLock = new Object();

                    @Override
                    public void collect(SeaTunnelRow row) {
                        fail("terminal reader cannot emit another row");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return checkpointLock;
                    }
                };
        AlphaVantageSource.Split end = new AlphaVantageSource.Split(config.fingerprint, 1, 0, null);
        try (AlphaVantageReader reader = new AlphaVantageReader(config, context, "AlphaVantage")) {
            reader.open();
            assertTrue(reader.snapshotState(0).isEmpty());
            reader.addSplits(
                    Collections.singletonList(
                            deserialize(serialize(end), AlphaVantageSource.Split.class)));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, reader.snapshotState(1).get(0).resource);
            verify(context).signalNoMoreElement();
        }
    }

    private static AlphaVantageConfig config(String token) {
        Map<String, Object> input = new HashMap<>();
        input.put("token", token);
        input.put("symbol", "IBM");
        input.put("resources", Collections.singletonList("time_series_daily"));
        return new AlphaVantageConfig(ReadonlyConfig.fromMap(input));
    }

    private static JsonNode series(String rows) throws IOException {
        return JSON.readTree(
                "{\"Meta Data\":{\"2. Symbol\":\"IBM\",\"5. Time Zone\":\"US/Eastern\"},"
                        + "\"Time Series (Daily)\":{"
                        + rows
                        + "}}");
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

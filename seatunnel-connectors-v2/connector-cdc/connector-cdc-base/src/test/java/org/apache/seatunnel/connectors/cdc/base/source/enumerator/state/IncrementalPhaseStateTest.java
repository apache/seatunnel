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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator.state;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

class IncrementalPhaseStateTest {

    @Test
    void shouldPreserveStopOffsetThroughJavaSerializationRoundTrip() throws Exception {
        Offset startupOffset = new TestOffset("startup", 1L);
        Offset stopOffset = new TestOffset("stop", 2L);
        IncrementalPhaseState state = new IncrementalPhaseState(startupOffset, stopOffset);

        IncrementalPhaseState restored = roundTrip(state);

        // A real checkpoint round-trip (plain Java serialization, the mechanism used for
        // enumerator state) must keep the resolved stop offset: otherwise a restore would
        // fall back to re-resolving latest() and drift the stop boundary.
        Assertions.assertEquals(startupOffset, restored.getStartupOffset());
        Assertions.assertEquals(stopOffset, restored.getStopOffset());
    }

    @Test
    void shouldDefaultStopOffsetToNullOnLegacySerializedState() throws Exception {
        // Simulates a checkpoint written before the stopOffset field existed (the single-arg
        // constructor is what old versions persisted): with the unchanged serialVersionUID,
        // Java field-evolution defaults the missing field to null, which falls through to the
        // pre-existing StopConfig resolution path.
        Offset startupOffset = new TestOffset("startup", 1L);
        IncrementalPhaseState legacy = new IncrementalPhaseState(startupOffset);

        IncrementalPhaseState restored = roundTrip(legacy);

        Assertions.assertEquals(startupOffset, restored.getStartupOffset());
        Assertions.assertNull(restored.getStopOffset());
    }

    @Test
    void shouldPreserveRemainingSplitsThroughJavaSerializationRoundTrip() throws Exception {
        Offset startupOffset = new TestOffset("startup", 1L);
        Offset stopOffset = new TestOffset("stop", 2L);
        IncrementalSplit remainingSplit =
                new IncrementalSplit(
                        "incremental-split-1",
                        Collections.singletonList(TableId.parse("database.schema.table")),
                        startupOffset,
                        stopOffset,
                        Collections.emptyList());
        IncrementalPhaseState state =
                new IncrementalPhaseState(
                        startupOffset, stopOffset, true, Collections.singletonList(remainingSplit));

        IncrementalPhaseState restored = roundTrip(state);

        Assertions.assertTrue(restored.isSplitAssigned());
        Assertions.assertEquals(1, restored.getRemainingSplits().size());
        Assertions.assertEquals(
                remainingSplit.splitId(), restored.getRemainingSplits().get(0).splitId());
        Assertions.assertEquals(
                remainingSplit.getTableIds(), restored.getRemainingSplits().get(0).getTableIds());
    }

    @Test
    void shouldPreserveAssignedTablesThroughJavaSerializationRoundTrip() throws Exception {
        Offset startupOffset = new TestOffset("startup", 1L);
        Offset stopOffset = new TestOffset("stop", 2L);
        Set<TableId> assignedTables =
                new LinkedHashSet<>(
                        Arrays.asList(
                                TableId.parse("database.schema.table1"),
                                TableId.parse("database.schema.table2")));
        IncrementalPhaseState state =
                new IncrementalPhaseState(
                        startupOffset, stopOffset, true, Collections.emptyList(), assignedTables);

        IncrementalPhaseState restored = roundTrip(state);

        Assertions.assertTrue(restored.isSplitAssigned());
        Assertions.assertTrue(restored.isAssignedTablesCheckpointed());
        Assertions.assertEquals(assignedTables, restored.getAssignedTables());
        Assertions.assertNotNull(restored.getRemainingSplits());
        Assertions.assertTrue(restored.getRemainingSplits().isEmpty());
    }

    @Test
    void shouldNormalizeMissingFieldsWhenDeserializingLegacyStream() throws Exception {
        Offset startupOffset = new TestOffset("startup", 1L);
        Offset stopOffset = new TestOffset("stop", 2L);
        LegacyIncrementalPhaseState legacy =
                new LegacyIncrementalPhaseState(startupOffset, stopOffset, true);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(legacy);
        }

        byte[] legacyBytes =
                rewriteClassName(
                        buffer.toByteArray(),
                        LegacyIncrementalPhaseState.class.getName(),
                        IncrementalPhaseState.class.getName());
        IncrementalPhaseState restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(legacyBytes))) {
            restored = (IncrementalPhaseState) input.readObject();
        }

        Assertions.assertEquals(startupOffset, restored.getStartupOffset());
        Assertions.assertEquals(stopOffset, restored.getStopOffset());
        Assertions.assertTrue(restored.isSplitAssigned());
        Assertions.assertNotNull(restored.getRemainingSplits());
        Assertions.assertTrue(restored.getRemainingSplits().isEmpty());
        Assertions.assertNotNull(restored.getAssignedTables());
        Assertions.assertTrue(restored.getAssignedTables().isEmpty());
        Assertions.assertFalse(restored.isAssignedTablesCheckpointed());
    }

    private static byte[] rewriteClassName(byte[] serialized, String fromName, String toName)
            throws Exception {
        byte[] fromToken = encodeUtfToken(fromName);
        byte[] toToken = encodeUtfToken(toName);
        int matchIndex = indexOf(serialized, fromToken);
        Assertions.assertTrue(matchIndex >= 0, "Class descriptor name not found in stream");
        ByteArrayOutputStream rewritten = new ByteArrayOutputStream(serialized.length);
        rewritten.write(serialized, 0, matchIndex);
        rewritten.write(toToken);
        rewritten.write(
                serialized,
                matchIndex + fromToken.length,
                serialized.length - matchIndex - fromToken.length);
        return rewritten.toByteArray();
    }

    private static byte[] encodeUtfToken(String value) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(out)) {
            byte[] utfBytes = value.getBytes(StandardCharsets.UTF_8);
            data.writeShort(utfBytes.length);
            data.write(utfBytes);
        }
        return out.toByteArray();
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    /**
     * Serialized representation of {@link IncrementalPhaseState} before {@code remainingSplits} and
     * {@code assignedTables} were added.
     */
    private static class LegacyIncrementalPhaseState implements Serializable {
        private static final long serialVersionUID = -6809026812298443356L;

        private final Offset startupOffset;
        private final Offset stopOffset;
        private final boolean splitAssigned;

        LegacyIncrementalPhaseState(
                Offset startupOffset, Offset stopOffset, boolean splitAssigned) {
            this.startupOffset = startupOffset;
            this.stopOffset = stopOffset;
            this.splitAssigned = splitAssigned;
        }
    }

    private static IncrementalPhaseState roundTrip(IncrementalPhaseState state) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(state);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            return (IncrementalPhaseState) input.readObject();
        }
    }

    /** A minimal serializable {@link Offset} implementation for round-trip tests. */
    private static class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        TestOffset(String file, long position) {
            Map<String, String> values = new HashMap<>();
            values.put("file", file);
            values.put("pos", String.valueOf(position));
            this.offset = values;
        }

        @Override
        public int compareTo(Offset that) {
            return this.offset.get("pos").compareTo(that.getOffset().get("pos"));
        }
    }
}

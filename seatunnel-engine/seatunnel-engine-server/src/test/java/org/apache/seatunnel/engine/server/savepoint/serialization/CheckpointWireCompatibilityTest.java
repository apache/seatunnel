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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.SubtaskStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Wire-format compatibility tests for the persisted checkpoint/savepoint model (mirrors the {@code
 * RecordSerializerTest} pattern for the pre-PR record wire contract).
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>{@code generateV0Fixtures} regenerates the {@code legacy-v0} wire bytes (produced by the
 *       current runtime classes, i.e. exactly what engines write today) into {@code
 *       target/fixtures/legacy-v0}; the committed copies under {@code
 *       src/test/resources/savepoint-wire/legacy-v0} are frozen - never edit after review.
 *   <li>{@code replayV0*} proves the frozen legacy bytes still decode into the {@code
 *       engine-wire-v1} DTO via {@link
 *       org.apache.seatunnel.engine.server.savepoint.serialization.LegacyCheckpointReader}.
 *   <li>{@code v1*} tests pin the new wire-format contract: stable enum names, no runtime-only
 *       fields, byte-stable round trip, explicit errors for unknown enum values.
 * </ul>
 */
public class CheckpointWireCompatibilityTest {

    private static final String LEGACY_VERSION = "legacy-v0";
    private static final Path GENERATED_DIR = Paths.get("target", "fixtures", LEGACY_VERSION);

    private final ProtoStuffSerializer serializer = new ProtoStuffSerializer();

    /** Regenerates the current wire bytes into the module build dir (for fixture archaeology). */
    @Test
    public void generateV0Fixtures() throws IOException {
        Files.createDirectories(GENERATED_DIR);
        Files.write(
                GENERATED_DIR.resolve("completed-checkpoint.ser"),
                serializer.serialize(CheckpointWireFixtures.sampleCompletedCheckpoint()));
        Files.write(
                GENERATED_DIR.resolve("completed-checkpoint-empty.ser"),
                serializer.serialize(CheckpointWireFixtures.sampleEmptyCompletedCheckpoint()));
        Files.write(
                GENERATED_DIR.resolve("pipeline-state.ser"),
                serializer.serialize(CheckpointWireFixtures.samplePipelineState()));
    }

    /** The current runtime layout must still reproduce the frozen legacy-v0 bytes exactly. */
    @Test
    public void legacyV0WireFormatIsStable() {
        Assertions.assertArrayEquals(
                CheckpointWireFixtures.LEGACY_V0_COMPLETED_CHECKPOINT,
                serializer.serialize(CheckpointWireFixtures.sampleCompletedCheckpoint()));
        Assertions.assertArrayEquals(
                CheckpointWireFixtures.LEGACY_V0_COMPLETED_CHECKPOINT_EMPTY,
                serializer.serialize(CheckpointWireFixtures.sampleEmptyCompletedCheckpoint()));
        Assertions.assertArrayEquals(
                CheckpointWireFixtures.LEGACY_V0_PIPELINE_STATE,
                serializer.serialize(CheckpointWireFixtures.samplePipelineState()));
    }

    @Test
    public void replayV0CompletedCheckpointViaLegacyReader() {
        WireCheckpoint wire =
                LegacyCheckpointReader.read(CheckpointWireFixtures.LEGACY_V0_COMPLETED_CHECKPOINT);
        assertFullSampleWire(wire);

        // The wire DTO must convert back into a fully-formed runtime model.
        CompletedCheckpoint checkpoint = CheckpointWireCodec.toCompletedCheckpoint(wire);
        Assertions.assertEquals(CheckpointWireFixtures.JOB_ID, checkpoint.getJobId());
        Assertions.assertEquals(CheckpointWireFixtures.PIPELINE_ID, checkpoint.getPipelineId());
        Assertions.assertEquals(CheckpointWireFixtures.CHECKPOINT_ID, checkpoint.getCheckpointId());
        Assertions.assertEquals(CheckpointType.SAVEPOINT_TYPE, checkpoint.getCheckpointType());
        Assertions.assertEquals(
                2,
                checkpoint
                        .getTaskStates()
                        .get(CheckpointWireFixtures.sampleActionStateKey())
                        .getParallelism());
        Assertions.assertNull(
                checkpoint
                        .getTaskStates()
                        .get(CheckpointWireFixtures.sampleActionStateKey())
                        .getSubtaskStates()
                        .get(1));
        Assertions.assertEquals(
                1, checkpoint.getTaskStatistics().get(0L).getNumAcknowledgedSubtasks());
    }

    @Test
    public void replayV0EmptyCheckpointViaLegacyReader() {
        WireCheckpoint wire =
                LegacyCheckpointReader.read(
                        CheckpointWireFixtures.LEGACY_V0_COMPLETED_CHECKPOINT_EMPTY);
        Assertions.assertEquals(
                CheckpointType.COMPLETED_POINT_TYPE.getName(), wire.getCheckpointTypeName());
        Assertions.assertTrue(wire.getTaskStates().isEmpty());
        Assertions.assertTrue(wire.getTaskStatistics().isEmpty());
    }

    @Test
    public void replayV0PipelineStateViaLegacyReader() {
        PipelineState state =
                serializer.deserialize(
                        CheckpointWireFixtures.LEGACY_V0_PIPELINE_STATE, PipelineState.class);
        Assertions.assertEquals(String.valueOf(CheckpointWireFixtures.JOB_ID), state.getJobId());
        Assertions.assertEquals(CheckpointWireFixtures.PIPELINE_ID, state.getPipelineId());
        Assertions.assertEquals(CheckpointWireFixtures.CHECKPOINT_ID, state.getCheckpointId());
        assertFullSampleWire(LegacyCheckpointReader.read(state.getStates()));
    }

    /** Runtime-only {@code isRestored} must not leak into the engine-wire-v1 contract. */
    @Test
    public void legacyIsRestoredIsNotPartOfV1Contract() throws IOException {
        CompletedCheckpoint cleared = CheckpointWireFixtures.sampleEmptyCompletedCheckpoint();
        cleared.setRestored(false);
        CompletedCheckpoint marked = CheckpointWireFixtures.sampleEmptyCompletedCheckpoint();
        marked.setRestored(true);
        Assertions.assertArrayEquals(
                CheckpointWireCodec.encode(CheckpointWireCodec.fromCompletedCheckpoint(cleared)),
                CheckpointWireCodec.encode(CheckpointWireCodec.fromCompletedCheckpoint(marked)));
    }

    /** The v1 codec must be byte-stable: decode(encode(x)) re-encodes to the same bytes. */
    @Test
    public void v1RoundTripIsByteStable() throws IOException {
        WireCheckpoint wire =
                CheckpointWireCodec.fromCompletedCheckpoint(
                        CheckpointWireFixtures.sampleCompletedCheckpoint());
        byte[] first = CheckpointWireCodec.encode(wire);
        byte[] second = CheckpointWireCodec.encode(CheckpointWireCodec.decode(first));
        Assertions.assertArrayEquals(first, second);
    }

    /** Enums are encoded by stable name, not ordinal, for every CheckpointType value. */
    @Test
    public void enumEncodingUsesStableNamesForAllCheckpointTypes() {
        for (CheckpointType type : CheckpointType.values()) {
            CompletedCheckpoint checkpoint =
                    new CompletedCheckpoint(
                            1L, 0, 1L, 1L, type, 1L, new HashMap<>(), new HashMap<>());
            WireCheckpoint wire = CheckpointWireCodec.fromCompletedCheckpoint(checkpoint);
            Assertions.assertEquals(type.getName(), wire.getCheckpointTypeName());
            CompletedCheckpoint round = CheckpointWireCodec.toCompletedCheckpoint(wire);
            Assertions.assertEquals(type, round.getCheckpointType());
        }
    }

    /** Unknown enum values must fail explicitly, not silently. */
    @Test
    public void unknownCheckpointTypeNameFailsExplicitly() {
        WireCheckpoint wire =
                new WireCheckpoint(
                        1, 0, 1L, 1L, "bogus-type", 1L, new HashMap<>(), new HashMap<>());
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> CheckpointWireCodec.toCompletedCheckpoint(wire));
        Assertions.assertTrue(exception.getMessage().contains("bogus-type"));
        Assertions.assertTrue(exception.getMessage().toLowerCase().contains("supported"));
    }

    private void assertFullSampleWire(WireCheckpoint wire) {
        Assertions.assertEquals(CheckpointWireFixtures.CHECKPOINT_ID, wire.getCheckpointId());
        Assertions.assertEquals(CheckpointWireFixtures.PIPELINE_ID, wire.getPipelineId());
        Assertions.assertEquals(CheckpointWireFixtures.JOB_ID, wire.getJobId());
        Assertions.assertEquals(
                CheckpointWireFixtures.TRIGGER_TIMESTAMP, wire.getTriggerTimestamp());
        Assertions.assertEquals(
                CheckpointType.SAVEPOINT_TYPE.getName(), wire.getCheckpointTypeName());
        Assertions.assertEquals(
                CheckpointWireFixtures.COMPLETED_TIMESTAMP, wire.getCompletedTimestamp());

        Assertions.assertEquals(1, wire.getTaskStates().size());
        WireActionState actionState =
                wire.getTaskStates().get(CheckpointWireFixtures.sampleActionStateKey().getName());
        Assertions.assertNotNull(actionState);
        Assertions.assertEquals(2, actionState.getParallelism());
        Assertions.assertEquals(2, actionState.getSubtaskStates().size());
        WireSubtaskState subtask0 = actionState.getSubtaskStates().get(0);
        Assertions.assertNotNull(subtask0);
        Assertions.assertEquals(0, subtask0.getIndex());
        Assertions.assertArrayEquals(new byte[] {1, 2, 3}, subtask0.getState().get(0));
        Assertions.assertNull(actionState.getSubtaskStates().get(1));
        WireSubtaskState coordinator = actionState.getCoordinatorState();
        Assertions.assertNotNull(coordinator);
        Assertions.assertEquals(-1, coordinator.getIndex());
        Assertions.assertArrayEquals(new byte[] {9, 8, 7}, coordinator.getState().get(0));

        Assertions.assertEquals(1, wire.getTaskStatistics().size());
        WireTaskStatistics statistics = wire.getTaskStatistics().get(0L);
        Assertions.assertEquals(0L, statistics.getJobVertexId());
        Assertions.assertEquals(2, statistics.getSubtaskStats().size());
        WireSubtaskStatistics acked = statistics.getSubtaskStats().get(0);
        Assertions.assertNotNull(acked);
        Assertions.assertEquals(0, acked.getSubtaskIndex());
        Assertions.assertEquals(1000L, acked.getAckTimestamp());
        Assertions.assertEquals(42L, acked.getStateSize());
        Assertions.assertEquals(SubtaskStatus.RUNNING.name(), acked.getSubtaskStatusName());
        Assertions.assertNull(statistics.getSubtaskStats().get(1));
        Assertions.assertArrayEquals(new boolean[] {true, false}, statistics.getSubtaskCompleted());
        Assertions.assertEquals(1, statistics.getNumAcknowledgedSubtasks());
        Assertions.assertNotNull(statistics.getLatestAckedSubtaskStatistics());
    }
}

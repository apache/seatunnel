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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Tests versioned checkpoint payload envelopes and legacy raw payload recovery. */
public class CompletedCheckpointCodecTest {

    private final ProtoStuffSerializer serializer = new ProtoStuffSerializer();

    @Test
    void testNormalCheckpointKeepsLegacyRawPayload() throws IOException {
        CompletedCheckpoint checkpoint = checkpoint();

        byte[] encoded = CompletedCheckpointCodec.encode(checkpoint, serializer);
        byte[] legacyPayload = serializer.serialize(checkpoint);
        CompletedCheckpoint decoded = CompletedCheckpointCodec.decode(encoded, serializer);

        Assertions.assertArrayEquals(legacyPayload, encoded);
        Assertions.assertEquals(checkpoint.getCheckpointId(), decoded.getCheckpointId());
        Assertions.assertTrue(decoded.getCheckpointIntent().isNormalCheckpoint());
    }

    @Test
    void testDecodeVersionedDynamicLookupCheckpointEnvelope() throws IOException {
        CompletedCheckpoint checkpoint = dynamicLookupCheckpoint();

        byte[] encoded = CompletedCheckpointCodec.encode(checkpoint, serializer);
        CompletedCheckpoint decoded = CompletedCheckpointCodec.decode(encoded, serializer);

        Assertions.assertEquals(checkpoint.getCheckpointId(), decoded.getCheckpointId());
        Assertions.assertEquals(checkpoint.getPipelineId(), decoded.getPipelineId());
        Assertions.assertEquals(
                CheckpointIntent.PURPOSE_DYNAMIC_LOOKUP_FACT_POSITION_ANCHOR,
                decoded.getCheckpointIntent().getCheckpointPurpose());
    }

    @Test
    void testDecodeLegacyRawCheckpointPayload() throws IOException {
        byte[] legacyPayload =
                serializer.serialize(
                        new LegacyCompletedCheckpoint(
                                1L,
                                2,
                                3L,
                                4L,
                                CheckpointType.CHECKPOINT_TYPE,
                                5L,
                                new HashMap<>(),
                                new HashMap<>()));
        CompletedCheckpoint decoded = CompletedCheckpointCodec.decode(legacyPayload, serializer);

        Assertions.assertEquals(3L, decoded.getCheckpointId());
        Assertions.assertEquals(2, decoded.getPipelineId());
        Assertions.assertTrue(decoded.getCheckpointIntent().isNormalCheckpoint());
    }

    @Test
    void testRejectDigestMismatch() throws IOException {
        byte[] encoded = CompletedCheckpointCodec.encode(dynamicLookupCheckpoint(), serializer);
        encoded[encoded.length - 1] = (byte) (encoded[encoded.length - 1] + 1);

        Assertions.assertThrows(
                IOException.class, () -> CompletedCheckpointCodec.decode(encoded, serializer));
    }

    private static CompletedCheckpoint checkpoint() {
        return new CompletedCheckpoint(
                1L,
                2,
                3L,
                4L,
                CheckpointType.CHECKPOINT_TYPE,
                5L,
                new HashMap<>(),
                new HashMap<>());
    }

    private static CompletedCheckpoint dynamicLookupCheckpoint() {
        return new CompletedCheckpoint(
                1L,
                2,
                3L,
                4L,
                CheckpointType.CHECKPOINT_TYPE,
                5L,
                new HashMap<>(),
                new HashMap<>(),
                CheckpointIntent.dynamicLookupFactPositionAnchor(1L, 2, 3L, new byte[] {1, 2, 3}));
    }

    /** Pre-change checkpoint shape used to verify raw legacy payload compatibility. */
    private static final class LegacyCompletedCheckpoint {
        private final long jobId;
        private final int pipelineId;
        private final long checkpointId;
        private final long triggerTimestamp;
        private final CheckpointType checkpointType;
        private final long completedTimestamp;
        private final Map<ActionStateKey, ActionState> taskStates;
        private final Map<Long, TaskStatistics> taskStatistics;
        private volatile boolean isRestored;

        private LegacyCompletedCheckpoint(
                long jobId,
                int pipelineId,
                long checkpointId,
                long triggerTimestamp,
                CheckpointType checkpointType,
                long completedTimestamp,
                Map<ActionStateKey, ActionState> taskStates,
                Map<Long, TaskStatistics> taskStatistics) {
            this.jobId = jobId;
            this.pipelineId = pipelineId;
            this.checkpointId = checkpointId;
            this.triggerTimestamp = triggerTimestamp;
            this.checkpointType = checkpointType;
            this.completedTimestamp = completedTimestamp;
            this.taskStates = taskStates;
            this.taskStatistics = taskStatistics;
        }
    }
}

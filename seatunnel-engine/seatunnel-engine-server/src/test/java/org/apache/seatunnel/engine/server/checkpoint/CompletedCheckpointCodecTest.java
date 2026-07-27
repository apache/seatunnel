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

/** Tests versioned checkpoint payload envelopes and legacy raw payload recovery. */
public class CompletedCheckpointCodecTest {

    private final ProtoStuffSerializer serializer = new ProtoStuffSerializer();

    @Test
    void testDecodeVersionedCheckpointEnvelope() throws IOException {
        CompletedCheckpoint checkpoint = checkpoint();

        byte[] encoded = CompletedCheckpointCodec.encode(checkpoint, serializer);
        CompletedCheckpoint decoded = CompletedCheckpointCodec.decode(encoded, serializer);

        Assertions.assertEquals(checkpoint.getCheckpointId(), decoded.getCheckpointId());
        Assertions.assertEquals(checkpoint.getPipelineId(), decoded.getPipelineId());
    }

    @Test
    void testDecodeLegacyRawCheckpointPayload() throws IOException {
        CompletedCheckpoint checkpoint = checkpoint();

        byte[] legacyPayload = serializer.serialize(checkpoint);
        CompletedCheckpoint decoded = CompletedCheckpointCodec.decode(legacyPayload, serializer);

        Assertions.assertEquals(checkpoint.getCheckpointId(), decoded.getCheckpointId());
    }

    @Test
    void testRejectDigestMismatch() throws IOException {
        byte[] encoded = CompletedCheckpointCodec.encode(checkpoint(), serializer);
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
}

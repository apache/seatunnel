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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.source.SourceGateState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;

/**
 * Guards the production fact-gate state codec against allowlist and checkpoint framing regressions.
 */
class SourceFlowLifeCycleGateStateTest {

    @Test
    void shouldRoundTripPreparedSplitsAndGateOwnership() throws Exception {
        byte[] splitBytes = {1, 2, 3};
        byte[] splitDigest = MessageDigest.getInstance("SHA-256").digest(splitBytes);
        for (boolean open : new boolean[] {false, true}) {
            SourceGateState original =
                    new SourceGateState(
                            open,
                            true,
                            Collections.singletonList(
                                    new SourceGateState.PreparedSplit(
                                            "orders-0", splitBytes, splitDigest)));
            SourceGateState restored =
                    SourceFlowLifeCycle.deserializeSourceGateState(
                            SourceFlowLifeCycle.serializeSourceGateState(original));

            Assertions.assertEquals(SourceGateState.CURRENT_VERSION, restored.getVersion());
            Assertions.assertEquals(open, restored.isGateOpen());
            Assertions.assertTrue(restored.isNoMoreSplits());
            Assertions.assertEquals(1, restored.getPreparedSplits().size());
            SourceGateState.PreparedSplit split = restored.getPreparedSplits().get(0);
            Assertions.assertEquals("orders-0", split.getSplitId());
            Assertions.assertArrayEquals(splitBytes, split.getSerializedSplit());
            Assertions.assertArrayEquals(splitDigest, split.getSerializedSplitDigest());
        }
    }

    @Test
    void shouldRejectUnlistedClassEvenWithValidEnvelopeDigest() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(payload)) {
            output.writeObject(new HashMap<>());
        }
        byte[] bytes = payload.toByteArray();
        // A correct frame and checksum must not bypass the class allowlist.
        byte[] envelope =
                ByteBuffer.allocate(Integer.BYTES * 3 + bytes.length + 32)
                        .putInt(0x53544754)
                        .putInt(1)
                        .putInt(bytes.length)
                        .put(bytes)
                        .put(MessageDigest.getInstance("SHA-256").digest(bytes))
                        .array();

        IOException error =
                Assertions.assertThrows(
                        IOException.class,
                        () -> SourceFlowLifeCycle.deserializeSourceGateState(envelope));
        Assertions.assertEquals(
                "Rejected dynamic lookup fact gate state class: java.util.HashMap",
                error.getMessage());
    }

    @Test
    void shouldRejectDamagedPayloadBeforeDeserialization() throws Exception {
        byte[] envelope =
                SourceFlowLifeCycle.serializeSourceGateState(
                        new SourceGateState(false, false, Collections.emptyList()));
        envelope[Integer.BYTES * 3] ^= 1;

        IOException error =
                Assertions.assertThrows(
                        IOException.class,
                        () -> SourceFlowLifeCycle.deserializeSourceGateState(envelope));
        Assertions.assertEquals(
                "Dynamic lookup fact gate state SHA-256 digest mismatch", error.getMessage());
    }
}

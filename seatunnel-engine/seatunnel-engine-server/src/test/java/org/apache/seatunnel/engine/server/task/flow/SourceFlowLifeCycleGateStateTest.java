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
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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
        // A correct frame and checksum must not bypass the class allowlist.
        byte[] envelope = validEnvelope(new HashMap<>());

        IOException error =
                Assertions.assertThrows(
                        IOException.class,
                        () -> SourceFlowLifeCycle.deserializeSourceGateState(envelope));
        Assertions.assertEquals(
                "Rejected dynamic lookup fact gate state class: java.util.HashMap",
                error.getMessage());
    }

    @Test
    void shouldRejectArrayDescriptorsOtherThanByteArray() throws Exception {
        // Empty arrays prove the descriptor itself is rejected before any element is visited. The
        // gate state only carries byte[] split bytes, so even an array of the allowlisted String
        // class stays outside the graph.
        for (Object array : new Object[] {new String[0], new HashMap<?, ?>[0]}) {
            byte[] envelope = validEnvelope(array);

            IOException error =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> SourceFlowLifeCycle.deserializeSourceGateState(envelope));
            Assertions.assertEquals(
                    "Rejected dynamic lookup fact gate state class: " + array.getClass().getName(),
                    error.getMessage());
        }
    }

    @Test
    void shouldRejectProxyClassesBeforeResolvingTheirHandler() throws Exception {
        // Proxy descriptors bypass resolveClass, so the stream needs its own proxy rejection.
        Object proxy =
                Proxy.newProxyInstance(
                        SourceFlowLifeCycleGateStateTest.class.getClassLoader(),
                        new Class<?>[] {Serializable.class},
                        new SerializableProxyHandler());
        byte[] envelope = validEnvelope(proxy);

        IOException error =
                Assertions.assertThrows(
                        IOException.class,
                        () -> SourceFlowLifeCycle.deserializeSourceGateState(envelope));
        Assertions.assertEquals(
                "Dynamic lookup fact gate state must not contain proxy classes",
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

    /**
     * Frames an arbitrary Java-serialized payload exactly like production, so that only the class
     * allowlist and proxy checks can reject it rather than the magic, version or digest checks.
     */
    private static byte[] validEnvelope(Object payload) throws Exception {
        ByteArrayOutputStream payloadStream = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(payloadStream)) {
            output.writeObject(payload);
        }
        byte[] bytes = payloadStream.toByteArray();
        return ByteBuffer.allocate(Integer.BYTES * 3 + bytes.length + 32)
                .putInt(0x53544754)
                .putInt(1)
                .putInt(bytes.length)
                .put(bytes)
                .put(MessageDigest.getInstance("SHA-256").digest(bytes))
                .array();
    }

    /** Benign serializable handler so a proxy instance can be written into a valid envelope. */
    private static final class SerializableProxyHandler implements InvocationHandler, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    }
}

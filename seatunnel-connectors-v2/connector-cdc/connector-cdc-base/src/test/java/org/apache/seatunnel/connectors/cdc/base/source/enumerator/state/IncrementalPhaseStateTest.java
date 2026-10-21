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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

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

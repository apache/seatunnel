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

package org.apache.seatunnel.engine.server.task.operation.source;

import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.Progress;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the wire contract of {@link ManagedSourceRegisterOperation#readInternal}: the reader
 * location must be validated against the factory/class id encoded on the wire, so a payload
 * carrying another registered type is rejected at the read site instead of being read positionally
 * into a mis-populated {@link TaskLocation}.
 */
public class ManagedSourceRegisterOperationSerializationTest {

    private static final TaskGroupLocation TASK_GROUP_LOCATION = new TaskGroupLocation(7L, 3, 11L);
    private static final TaskLocation ENUMERATOR_LOCATION =
            new TaskLocation(TASK_GROUP_LOCATION, 20000L, 0);
    private static final TaskLocation READER_LOCATION =
            new TaskLocation(TASK_GROUP_LOCATION, 30000L, 2);

    private InternalSerializationService serializationService;

    @BeforeEach
    void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    /**
     * Regression guard: if the read path stops consuming a genuine TaskLocation payload, or drops
     * or reorders a field, the re-encoded bytes no longer match the original ones.
     */
    @Test
    void testReaderLocationPayloadRoundTrips() throws IOException {
        ManagedSourceRegisterOperation original =
                new ManagedSourceRegisterOperation(
                        ENUMERATOR_LOCATION,
                        READER_LOCATION,
                        42L,
                        "attempt-1",
                        1,
                        "digest",
                        5L,
                        9L,
                        2L);
        byte[] originalBytes = encode(original::writeInternal);

        ManagedSourceRegisterOperation decoded = new ManagedSourceRegisterOperation();
        decoded.readInternal(serializationService.createObjectDataInput(originalBytes));

        assertEquals(ENUMERATOR_LOCATION, decoded.getTaskLocation());
        assertArrayEquals(originalBytes, encode(decoded::writeInternal));
    }

    /**
     * Regression guard: if the read path goes back to overriding the wire type (or stops checking
     * it), a payload whose reader-location slot carries another class id of the same serializer
     * factory is no longer rejected with the explicit type-mismatch error.
     */
    @Test
    void testMismatchedReaderLocationTypeIsRejected() throws IOException {
        MismatchedReaderLocationOperation mismatched =
                new MismatchedReaderLocationOperation(ENUMERATOR_LOCATION);
        byte[] mismatchedBytes = encode(mismatched::writeInternal);

        ManagedSourceRegisterOperation decoded = new ManagedSourceRegisterOperation();
        IOException exception =
                assertThrows(
                        IOException.class,
                        () ->
                                decoded.readInternal(
                                        serializationService.createObjectDataInput(
                                                mismatchedBytes)));

        assertEquals(
                "Managed Source register operation expects reader location of type "
                        + TaskLocation.class.getName()
                        + " but the wire payload decoded to "
                        + Progress.class.getName(),
                exception.getMessage());
    }

    private byte[] encode(WireWritable operation) throws IOException {
        try (BufferObjectDataOutput out = serializationService.createObjectDataOutput()) {
            operation.writeTo(out);
            return out.toByteArray();
        }
    }

    /** Adapts the protected writeInternal of an operation to a shared byte-encoding helper. */
    @FunctionalInterface
    private interface WireWritable {
        void writeTo(ObjectDataOutput out) throws IOException;
    }

    /**
     * Writes the same field layout as {@link ManagedSourceRegisterOperation} but encodes a {@link
     * Progress}, which shares the serializer factory id with {@link TaskLocation} and differs only
     * in class id, where the reader location is expected.
     */
    private static class MismatchedReaderLocationOperation extends TaskOperation {

        MismatchedReaderLocationOperation(TaskLocation enumeratorLocation) {
            super(enumeratorLocation);
        }

        @Override
        public void runInternal() {
            throw new UnsupportedOperationException("Serialization fixture only");
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(new Progress());
            out.writeLong(42L);
            out.writeString("attempt-1");
            out.writeInt(1);
            out.writeString("digest");
            out.writeLong(5L);
            out.writeLong(9L);
            out.writeLong(2L);
        }

        @Override
        public int getFactoryId() {
            throw new UnsupportedOperationException("Serialization fixture only");
        }

        @Override
        public int getClassId() {
            throw new UnsupportedOperationException("Serialization fixture only");
        }
    }
}

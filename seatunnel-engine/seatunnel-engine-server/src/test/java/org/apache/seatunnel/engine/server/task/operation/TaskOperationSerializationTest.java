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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.Progress;
import org.apache.seatunnel.engine.server.task.operation.source.AssignSplitOperation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskOperationSerializationTest {

    private static final TaskLocation LOCATION =
            new TaskLocation(new TaskGroupLocation(7L, 3, 11L), 5L, 2);
    private final InternalSerializationService serializationService =
            new DefaultSerializationServiceBuilder().build();

    @AfterEach
    void tearDown() {
        serializationService.dispose();
    }

    @Test
    void taskLocationRoundTripsWithoutChangingWireBytes() throws IOException {
        TaskFixture original = new TaskFixture(LOCATION);
        byte[] bytes = encode(original::writeInternal);
        TaskFixture decoded = new TaskFixture(null);
        decoded.readInternal(serializationService.createObjectDataInput(bytes));
        assertEquals(LOCATION, decoded.getTaskLocation());
        assertArrayEquals(bytes, encode(decoded::writeInternal));
    }

    @Test
    void splitAssignmentRoundTripsWithoutChangingWireBytes() throws IOException {
        SplitFixture original = new SplitFixture(LOCATION);
        byte[] bytes = encode(original::writeWire);
        SplitFixture decoded = new SplitFixture(null);
        decoded.readWire(serializationService.createObjectDataInput(bytes));
        assertArrayEquals(bytes, encode(decoded::writeWire));
    }

    @Test
    void taskOperationRejectsDifferentClassIdFromSameFactory() throws IOException {
        assertRejected(new WrongLocation(), false, Progress.class.getName());
    }

    @Test
    void splitAssignmentRejectsDifferentClassIdFromSameFactory() throws IOException {
        assertRejected(new WrongLocation(), true, Progress.class.getName());
    }

    @Test
    void taskOperationRejectsNullLocation() throws IOException {
        assertRejected(null, false, "null");
    }

    @Test
    void splitAssignmentRejectsNullLocation() throws IOException {
        assertRejected(null, true, "null");
    }

    private void assertRejected(Object location, boolean splitAssignment, String decodedType)
            throws IOException {
        byte[] bytes = encode(new WireFixture(location, splitAssignment)::writeInternal);
        IOException error =
                assertThrows(
                        IOException.class,
                        () -> {
                            ObjectDataInput input =
                                    serializationService.createObjectDataInput(bytes);
                            if (splitAssignment) {
                                new SplitFixture(null).readWire(input);
                            } else {
                                new TaskFixture(null).readInternal(input);
                            }
                        });
        assertTrue(error.getMessage().contains(TaskLocation.class.getName()));
        assertTrue(error.getMessage().contains(decodedType));
    }

    private byte[] encode(WireWriter writer) throws IOException {
        try (BufferObjectDataOutput output = serializationService.createObjectDataOutput()) {
            writer.write(output);
            return output.toByteArray();
        }
    }

    @FunctionalInterface
    private interface WireWriter {
        void write(ObjectDataOutput output) throws IOException;
    }

    private static class TaskFixture extends TaskOperation {
        TaskFixture(TaskLocation location) {
            super(location);
        }

        @Override
        public void runInternal() {}

        @Override
        public int getFactoryId() {
            return TaskDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return TaskDataSerializerHook.RESTORED_SPLIT_OPERATOR;
        }
    }

    private static class SplitFixture extends AssignSplitOperation {
        SplitFixture(TaskLocation location) {
            super(location, Arrays.asList(new byte[] {1, 2}, new byte[] {3}));
        }

        void readWire(ObjectDataInput input) throws IOException {
            super.readInternal(input);
        }

        void writeWire(ObjectDataOutput output) throws IOException {
            super.writeInternal(output);
        }
    }

    private static class WireFixture extends TracingOperation {
        private final Object location;
        private final boolean splitAssignment;

        WireFixture(Object location, boolean splitAssignment) {
            this.location = location;
            this.splitAssignment = splitAssignment;
        }

        @Override
        public void runInternal() {}

        @Override
        protected void writeInternal(ObjectDataOutput output) throws IOException {
            super.writeInternal(output);
            if (splitAssignment) {
                output.writeInt(0);
            }
            output.writeObject(location);
        }
    }

    // A real registered factory/class id with TaskLocation-compatible field bytes.
    // A class override must not turn this payload into a valid TaskLocation.
    private static class WrongLocation implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return TaskDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return TaskDataSerializerHook.PROGRESS_TYPE;
        }

        @Override
        public void writeData(ObjectDataOutput output) throws IOException {
            LOCATION.writeData(output);
        }

        @Override
        public void readData(ObjectDataInput input) {
            throw new UnsupportedOperationException();
        }
    }
}

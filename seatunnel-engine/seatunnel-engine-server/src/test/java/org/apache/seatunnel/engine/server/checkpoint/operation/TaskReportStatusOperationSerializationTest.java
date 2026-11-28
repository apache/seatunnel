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

package org.apache.seatunnel.engine.server.checkpoint.operation;

import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TracingOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.nio.ByteOrder;

class TaskReportStatusOperationSerializationTest {

    private static final int OBSERVED_FACTORY_ID = 77501;

    private static final TaskLocation LOCATION =
            new TaskLocation(new TaskGroupLocation(7L, 3, 11L), 5L, 2);

    private InternalSerializationService service;

    @BeforeEach
    void setUp() {
        service =
                new DefaultSerializationServiceBuilder()
                        .addDataSerializableFactory(
                                OBSERVED_FACTORY_ID, id -> new ObservedLocation())
                        .build();
    }

    @AfterEach
    void tearDown() {
        service.dispose();
    }

    @Test
    void testValidReportsPreserveLocationStatusAndWireBytes() {
        for (SeaTunnelTaskState status : SeaTunnelTaskState.values()) {
            Data data = service.toData(new TaskReportStatusOperation(LOCATION, status));
            TaskReportStatusOperation restored = service.toObject(data);

            Assertions.assertEquals(LOCATION, restored.getLocation());
            Assertions.assertEquals(status, restored.getStatus());
            Assertions.assertArrayEquals(
                    data.toByteArray(), service.toData(restored).toByteArray());
        }
    }

    @Test
    void testRejectsDifferentClassFromTaskLocationFactory() {
        // The old typed reader accepts this registered Progress ID with TaskLocation-shaped fields.
        assertInvalidLocation(
                new WrongLocationWireType(), "class ID is " + TaskDataSerializerHook.PROGRESS_TYPE);
    }

    @Test
    void testRejectsUnrelatedLocationValue() {
        assertInvalidLocation("not a task location", "wire serializer type");
    }

    @Test
    void testRejectsNullLocation() {
        assertInvalidLocation(null, "null");
    }

    @Test
    void testRejectsNullStatusBeforeExecution() {
        Data data = service.toData(new TaskReportStatusOperation(LOCATION, null));
        HazelcastSerializationException exception =
                Assertions.assertThrows(
                        HazelcastSerializationException.class, () -> service.toObject(data));
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Assertions.assertInstanceOf(IOException.class, cause);
        Assertions.assertTrue(cause.getMessage().contains("null status"));
    }

    @Test
    void testRejectsClassNamedLocationBeforeConstruction() {
        Data data = service.toData(new ReportWire(new SideEffectLocation()));
        SideEffectLocation.constructions = 0;
        SideEffectLocation.reads = 0;
        assertDecodeFailure(data, "expects an identified");
        Assertions.assertEquals(0, SideEffectLocation.constructions);
        Assertions.assertEquals(0, SideEffectLocation.reads);
    }

    @Test
    void testRejectsUnexpectedFactoryBeforeConstruction() {
        Data data = service.toData(new ReportWire(new ObservedLocation()));
        SideEffectLocation.constructions = 0;
        SideEffectLocation.reads = 0;
        Assertions.assertThrows(
                HazelcastSerializationException.class, () -> service.toObject(data));
        Assertions.assertEquals(0, SideEffectLocation.constructions);
        Assertions.assertEquals(0, SideEffectLocation.reads);
    }

    @Test
    void testValidReportsPreserveLittleEndianBytes() {
        InternalSerializationService littleEndian =
                new DefaultSerializationServiceBuilder()
                        .setByteOrder(ByteOrder.LITTLE_ENDIAN)
                        .setAllowUnsafe(false)
                        .build();
        try {
            for (SeaTunnelTaskState status : SeaTunnelTaskState.values()) {
                Data data = littleEndian.toData(new TaskReportStatusOperation(LOCATION, status));
                TaskReportStatusOperation restored = littleEndian.toObject(data);
                Assertions.assertEquals(LOCATION, restored.getLocation());
                Assertions.assertEquals(status, restored.getStatus());
                Assertions.assertArrayEquals(
                        data.toByteArray(), littleEndian.toData(restored).toByteArray());
            }
        } finally {
            littleEndian.dispose();
        }
    }

    public static class ObservedLocation extends SideEffectLocation
            implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return OBSERVED_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return TaskDataSerializerHook.TASK_LOCATION_TYPE;
        }
    }

    public static class SideEffectLocation implements DataSerializable {
        private static int constructions;
        private static int reads;

        public SideEffectLocation() {
            constructions++;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            LOCATION.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            reads++;
            new TaskLocation().readData(in);
        }
    }

    private void assertInvalidLocation(Object location, String actualType) {
        Data data = service.toData(new ReportWire(location));
        assertDecodeFailure(data, actualType);
    }

    private void assertDecodeFailure(Data data, String actualType) {
        HazelcastSerializationException exception =
                Assertions.assertThrows(
                        HazelcastSerializationException.class, () -> service.toObject(data));
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Assertions.assertInstanceOf(IOException.class, cause);
        Assertions.assertTrue(cause.getMessage().contains(TaskLocation.class.getName()));
        Assertions.assertTrue(cause.getMessage().contains(actualType));
    }

    /** Writes the real operation envelope while allowing a malformed location field. */
    private static class ReportWire extends TracingOperation implements IdentifiedDataSerializable {
        private final Object location;

        private ReportWire(Object location) {
            this.location = location;
        }

        @Override
        public int getFactoryId() {
            return CheckpointDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CheckpointDataSerializerHook.TASK_REPORT_STATUS_OPERATOR;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(location);
            out.writeObject(SeaTunnelTaskState.RUNNING);
        }

        @Override
        public void runInternal() {}
    }

    /** Uses the production factory, with a different class ID but location-compatible fields. */
    private static class WrongLocationWireType implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return TaskDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return TaskDataSerializerHook.PROGRESS_TYPE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            LOCATION.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in) {
            throw new UnsupportedOperationException("Write-only malformed fixture");
        }
    }
}

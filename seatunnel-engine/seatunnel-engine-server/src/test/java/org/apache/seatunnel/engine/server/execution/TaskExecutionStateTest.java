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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.exception.SchemaChangePolicyException;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

/**
 * Protects non-retryable failure propagation and legacy worker/master state serialization.
 *
 * <p>Old stream descriptors default the new flag to false, while newly serialized states retain
 * explicit policy failures.
 */
class TaskExecutionStateTest {

    private static final TaskGroupLocation LOCATION = new TaskGroupLocation(1L, 1, 1L);

    @Test
    void preservesNonRetryableMarkerFromNestedCause() {
        SchemaChangePolicyException policyFailure =
                new SchemaChangePolicyException(
                        SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                        "deterministic policy rejection",
                        TableIdentifier.of("catalog", "database", "table"),
                        "job-under-test");

        TaskExecutionState state =
                new TaskExecutionState(
                        LOCATION,
                        ExecutionState.FAILED,
                        new RuntimeException("task wrapper", policyFailure));

        Assertions.assertTrue(state.isNonRetryable());
        Assertions.assertTrue(state.getThrowableMsg().contains("deterministic policy rejection"));
    }

    @Test
    void ordinaryFailureRemainsRetryable() {
        TaskExecutionState state =
                new TaskExecutionState(
                        LOCATION, ExecutionState.FAILED, new RuntimeException("transient"));

        Assertions.assertFalse(state.isNonRetryable());
    }

    @Test
    void readsLegacyDescriptorWithoutNonRetryableField() throws Exception {
        // Manually encoded from the released three-field descriptor and its default Java UID.
        // Equivalent to the old constructor (null, null, "legacy failure"); no new flag is present.
        byte[] legacy =
                Base64.getDecoder()
                        .decode(
                                "rO0ABXNyAD9vcmcuYXBhY2hlLnNlYXR1bm5lbC5lbmdpbmUuc2VydmVyLmV4ZWN1dGlvbi5UYXNrRXhlY3V0aW9uU3RhdGX+ff2Q8TwWZwIAA0wADmV4ZWN1dGlvblN0YXRldAA9TG9yZy9hcGFjaGUvc2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0aW9uL0V4ZWN1dGlvblN0YXRlO0wAEXRhc2tHcm91cExvY2F0aW9udABATG9yZy9hcGFjaGUvc2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0aW9uL1Rhc2tHcm91cExvY2F0aW9uO0wADHRocm93YWJsZU1zZ3QAEkxqYXZhL2xhbmcvU3RyaW5nO3hwcHB0AA5sZWdhY3kgZmFpbHVyZQ==");
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(legacy))) {
            TaskExecutionState restored = (TaskExecutionState) input.readObject();
            Assertions.assertEquals("legacy failure", restored.getThrowableMsg());
            Assertions.assertFalse(restored.isNonRetryable());
            Assertions.assertNull(restored.getTaskGroupLocation());
            Assertions.assertNull(restored.getExecutionState());
        }
    }

    @Test
    void preservesNonRetryableMarkerDuringSerialization() throws Exception {
        TaskExecutionState state =
                new TaskExecutionState(
                        LOCATION, ExecutionState.FAILED, new RuntimeException("transient"), true);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(state);
        }

        TaskExecutionState restored;
        try (ObjectInputStream objectInput =
                new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()))) {
            restored = (TaskExecutionState) objectInput.readObject();
        }

        Assertions.assertTrue(restored.isNonRetryable());
        Assertions.assertEquals(LOCATION, restored.getTaskGroupLocation());
        Assertions.assertEquals(ExecutionState.FAILED, restored.getExecutionState());
    }
}

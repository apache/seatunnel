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
import java.io.ObjectStreamClass;

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
    void preservesLegacySerialVersionUid() {
        Assertions.assertEquals(
                -108652017022658969L,
                ObjectStreamClass.lookup(TaskExecutionState.class).getSerialVersionUID());
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

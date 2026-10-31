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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TaskExecutionStateTest {
    @Test
    void readsLegacyTerminalReportWithoutExecutionId() throws Exception {
        // Serialized with the previous TaskExecutionState class and its original serialVersionUID.
        byte[] bytes =
                Base64.getDecoder()
                        .decode(
                                "rO0ABXNyAD9vcmcuYXBhY2hlLnNlYXR1bm5lbC5lbmdpbmUuc2VydmVyLmV4ZWN1dGlvbi5UYXNrRXhlY3V0aW9uU3RhdGX+ff2Q8TwWZwIAA0wADmV4ZWN1dGlvblN0YXRldAA9TG9yZy9hcGFjaGUvc2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0aW9uL0V4ZWN1dGlvblN0YXRlO0wAEXRhc2tHcm91cExvY2F0aW9udABATG9yZy9hcGFjaGUvc2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0aW9uL1Rhc2tHcm91cExvY2F0aW9uO0wADHRocm93YWJsZU1zZ3QAEkxqYXZhL2xhbmcvU3RyaW5nO3hwfnIAO29yZy5hcGFjaGUuc2VhdHVubmVsLmVuZ2luZS5zZXJ2ZXIuZXhlY3V0aW9uLkV4ZWN1dGlvblN0YXRlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAIQ0FOQ0VMRURzcgA+b3JnLmFwYWNoZS5zZWF0dW5uZWwuZW5naW5lLnNlcnZlci5leGVjdXRpb24uVGFza0dyb3VwTG9jYXRpb26Mg/+L8j7j+QIAA0oABWpvYklkSQAKcGlwZWxpbmVJZEoAC3Rhc2tHcm91cElkeHAAAAAAAAAABwAAAAEAAAAAAAAAAnA=");
        TaskExecutionState state =
                (TaskExecutionState)
                        new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
        assertEquals(new TaskGroupLocation(7L, 1, 2L), state.getTaskGroupLocation());
        assertEquals(ExecutionState.CANCELED, state.getExecutionState());
        assertNull(state.getExecutionId());
    }

    @Test
    void terminalReportRetainsAttemptAndFailureAcrossSerialization() throws Exception {
        TaskExecutionState original =
                new TaskExecutionState(
                                new TaskGroupLocation(7L, 1, 2L),
                                ExecutionState.FAILED,
                                "failed attempt")
                        .withExecutionId(41L);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new ObjectOutputStream(bytes).writeObject(original);
        TaskExecutionState state =
                (TaskExecutionState)
                        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))
                                .readObject();
        assertEquals(41L, state.getExecutionId());
        assertEquals("failed attempt", state.getThrowableMsg());
        assertEquals(ExecutionState.FAILED, state.getExecutionState());
        assertEquals(original.getTaskGroupLocation(), state.getTaskGroupLocation());
    }
}

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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Base64;

/**
 * Verifies that task state messages exchanged between workers and the master keep the Java
 * serialization form they had before their serialVersionUID was declared.
 *
 * <p>The fixtures were written by the classes without a declared serialVersionUID, on JDK 8 and JDK
 * 11, which produced identical bytes.
 */
class TaskStateSerializationTest {

    private static final String TASK_EXECUTION_STATE_FAILED =
            "rO0ABXNyAD9vcmcuYXBhY2hlLnNlYXR1bm5lbC5lbmdpbmUuc2VydmVyLmV4ZWN1dGlvbi5UYXNr"
                    + "RXhlY3V0aW9uU3RhdGX+ff2Q8TwWZwIAA0wADmV4ZWN1dGlvblN0YXRldAA9TG9yZy9hcGFjaGUv"
                    + "c2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0aW9uL0V4ZWN1dGlvblN0YXRlO0wAEXRhc2tH"
                    + "cm91cExvY2F0aW9udABATG9yZy9hcGFjaGUvc2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0"
                    + "aW9uL1Rhc2tHcm91cExvY2F0aW9uO0wADHRocm93YWJsZU1zZ3QAEkxqYXZhL2xhbmcvU3RyaW5n"
                    + "O3hwfnIAO29yZy5hcGFjaGUuc2VhdHVubmVsLmVuZ2luZS5zZXJ2ZXIuZXhlY3V0aW9uLkV4ZWN1"
                    + "dGlvblN0YXRlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAGRkFJ"
                    + "TEVEc3IAPm9yZy5hcGFjaGUuc2VhdHVubmVsLmVuZ2luZS5zZXJ2ZXIuZXhlY3V0aW9uLlRhc2tH"
                    + "cm91cExvY2F0aW9ujIP/i/I+4/kCAANKAAVqb2JJZEkACnBpcGVsaW5lSWRKAAt0YXNrR3JvdXBJ"
                    + "ZHhwAAAAAAAAA+kAAAABAAAAAAAATiF0AC5qYXZhLmxhbmcuSWxsZWdhbFN0YXRlRXhjZXB0aW9u"
                    + "OiBzb3VyY2UgZmFpbGVk";

    private static final String TASK_EXECUTION_STATE_FINISHED =
            "rO0ABXNyAD9vcmcuYXBhY2hlLnNlYXR1bm5lbC5lbmdpbmUuc2VydmVyLmV4ZWN1dGlvbi5UYXNr"
                    + "RXhlY3V0aW9uU3RhdGX+ff2Q8TwWZwIAA0wADmV4ZWN1dGlvblN0YXRldAA9TG9yZy9hcGFjaGUv"
                    + "c2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0aW9uL0V4ZWN1dGlvblN0YXRlO0wAEXRhc2tH"
                    + "cm91cExvY2F0aW9udABATG9yZy9hcGFjaGUvc2VhdHVubmVsL2VuZ2luZS9zZXJ2ZXIvZXhlY3V0"
                    + "aW9uL1Rhc2tHcm91cExvY2F0aW9uO0wADHRocm93YWJsZU1zZ3QAEkxqYXZhL2xhbmcvU3RyaW5n"
                    + "O3hwfnIAO29yZy5hcGFjaGUuc2VhdHVubmVsLmVuZ2luZS5zZXJ2ZXIuZXhlY3V0aW9uLkV4ZWN1"
                    + "dGlvblN0YXRlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAIRklO"
                    + "SVNIRURzcgA+b3JnLmFwYWNoZS5zZWF0dW5uZWwuZW5naW5lLnNlcnZlci5leGVjdXRpb24uVGFz"
                    + "a0dyb3VwTG9jYXRpb26Mg/+L8j7j+QIAA0oABWpvYklkSQAKcGlwZWxpbmVJZEoAC3Rhc2tHcm91"
                    + "cElkeHAAAAAAAAAD6QAAAAEAAAAAAABOIXA=";

    private static final String TASK_DEPLOY_STATE_SUCCESS =
            "rO0ABXNyADxvcmcuYXBhY2hlLnNlYXR1bm5lbC5lbmdpbmUuc2VydmVyLmV4ZWN1dGlvbi5UYXNr"
                    + "RGVwbG95U3RhdGUkuMNro2ihAgIAAloAB3N1Y2Nlc3NMAAx0aHJvd2FibGVNc2d0ABJMamF2YS9s"
                    + "YW5nL1N0cmluZzt4cAFw";

    private static final String TASK_DEPLOY_STATE_FAILED =
            "rO0ABXNyADxvcmcuYXBhY2hlLnNlYXR1bm5lbC5lbmdpbmUuc2VydmVyLmV4ZWN1dGlvbi5UYXNr"
                    + "RGVwbG95U3RhdGUkuMNro2ihAgIAAloAB3N1Y2Nlc3NMAAx0aHJvd2FibGVNc2d0ABJMamF2YS9s"
                    + "YW5nL1N0cmluZzt4cAB0AC5qYXZhLmxhbmcuSWxsZWdhbFN0YXRlRXhjZXB0aW9uOiBkZXBsb3kg"
                    + "ZmFpbGVk";

    private static final TaskGroupLocation LOCATION = new TaskGroupLocation(1001L, 1, 20001L);

    @Test
    void testDeclaredSerialVersionUidsMatchPreviouslyGeneratedValues() {
        Assertions.assertEquals(
                -108652017022658969L,
                ObjectStreamClass.lookup(TaskExecutionState.class).getSerialVersionUID());
        Assertions.assertEquals(
                2646079648150626562L,
                ObjectStreamClass.lookup(TaskDeployState.class).getSerialVersionUID());
    }

    @Test
    void testTaskExecutionStateReadsPreviousWireForm() throws Exception {
        TaskExecutionState failed = (TaskExecutionState) deserialize(TASK_EXECUTION_STATE_FAILED);
        Assertions.assertEquals(LOCATION, failed.getTaskGroupLocation());
        Assertions.assertEquals(ExecutionState.FAILED, failed.getExecutionState());
        Assertions.assertEquals(
                "java.lang.IllegalStateException: source failed", failed.getThrowableMsg());

        TaskExecutionState finished =
                (TaskExecutionState) deserialize(TASK_EXECUTION_STATE_FINISHED);
        Assertions.assertEquals(LOCATION, finished.getTaskGroupLocation());
        Assertions.assertEquals(ExecutionState.FINISHED, finished.getExecutionState());
        Assertions.assertNull(finished.getThrowableMsg());
    }

    @Test
    void testTaskExecutionStateWritesPreviousWireForm() throws Exception {
        Assertions.assertEquals(
                TASK_EXECUTION_STATE_FAILED,
                serialize(
                        new TaskExecutionState(
                                LOCATION,
                                ExecutionState.FAILED,
                                "java.lang.IllegalStateException: source failed")));
        Assertions.assertEquals(
                TASK_EXECUTION_STATE_FINISHED,
                serialize(new TaskExecutionState(LOCATION, ExecutionState.FINISHED)));
    }

    @Test
    void testTaskDeployStateReadsPreviousWireForm() throws Exception {
        TaskDeployState success = (TaskDeployState) deserialize(TASK_DEPLOY_STATE_SUCCESS);
        Assertions.assertTrue(success.isSuccess());
        Assertions.assertNull(success.getThrowableMsg());

        TaskDeployState failed = (TaskDeployState) deserialize(TASK_DEPLOY_STATE_FAILED);
        Assertions.assertFalse(failed.isSuccess());
        Assertions.assertEquals(
                "java.lang.IllegalStateException: deploy failed", failed.getThrowableMsg());
    }

    @Test
    void testTaskDeployStateWritesPreviousWireForm() throws Exception {
        Assertions.assertEquals(TASK_DEPLOY_STATE_SUCCESS, serialize(TaskDeployState.success()));
        Assertions.assertEquals(
                TASK_DEPLOY_STATE_FAILED,
                serialize(
                        new TaskDeployState(
                                false, "java.lang.IllegalStateException: deploy failed")));
    }

    private static String serialize(Object value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private static Object deserialize(String base64) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(
                        new ByteArrayInputStream(Base64.getDecoder().decode(base64)))) {
            return input.readObject();
        }
    }
}

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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the failure-message classification in {@link PhysicalVertex}. */
public class PhysicalVertexTest {

    /**
     * Only the coordinator's exact offline-node message should be treated as an expected scale-down
     * failure.
     */
    @Test
    public void shouldMatchOnlyCoordinatorOfflineFailureMessage() {
        Assertions.assertTrue(
                PhysicalVertex.isDeployedNodeOfflineFailure(
                        "The taskGroup(taskGroupLocation) deployed node(127.0.0.1:5801) offline"));
        Assertions.assertFalse(
                PhysicalVertex.isDeployedNodeOfflineFailure(
                        "The taskGroup(taskGroupLocation) deployed node(127.0.0.1:5801) restarted"));
        Assertions.assertFalse(
                PhysicalVertex.isDeployedNodeOfflineFailure(
                        "deployed node(127.0.0.1:5801) offline"));
        Assertions.assertFalse(
                PhysicalVertex.isDeployedNodeOfflineFailure(
                        "The taskGroup(taskGroupLocation) deployed node(127.0.0.1:5801) offline"
                                + " while retrying"));
    }

    @Test
    public void shouldMatchCoordinatorOfflineFailureAfterExceptionFlattening() {
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 2, 3L);
        String deployedAddress = "127.0.0.1:5801";
        String throwableMsg =
                new TaskExecutionState(
                                taskGroupLocation,
                                ExecutionState.FAILED,
                                new JobException(
                                        String.format(
                                                "The taskGroup(%s) deployed node(%s) offline",
                                                taskGroupLocation, deployedAddress)))
                        .getThrowableMsg();

        Assertions.assertTrue(PhysicalVertex.isDeployedNodeOfflineFailure(throwableMsg));
    }
}

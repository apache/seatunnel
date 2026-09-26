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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.server.execution.ExecutionState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

/**
 * Covers {@link CoordinatorService#resolveLostMemberState(ExecutionState)}, the decision applied to
 * every task vertex whose worker leaves the cluster: which terminal state the master assigns
 * locally now that the worker can no longer report one itself.
 *
 * <p>Guards the regression fixed alongside {@code
 * SplitClusterFaultToleranceIT#testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck}: a
 * vertex that was already CANCELING (the user asked for the cancel; the worker acked the {@code
 * CancelTaskOperation} but died before its terminal callback) must resolve to CANCELED, not FAILED,
 * so a user-cancelled job is not reported as failed just because a worker was lost while honoring
 * the cancel. Vertices that were still DEPLOYING or RUNNING keep resolving to FAILED, and vertices
 * in any other state are left untouched.
 */
class CoordinatorServiceLostMemberResolutionTest {

    @Test
    void cancelingVertexResolvesToCanceled() {
        Assertions.assertEquals(
                ExecutionState.CANCELED,
                CoordinatorService.resolveLostMemberState(ExecutionState.CANCELING));
    }

    @Test
    void deployingAndRunningVerticesStillResolveToFailed() {
        Assertions.assertEquals(
                ExecutionState.FAILED,
                CoordinatorService.resolveLostMemberState(ExecutionState.DEPLOYING));
        Assertions.assertEquals(
                ExecutionState.FAILED,
                CoordinatorService.resolveLostMemberState(ExecutionState.RUNNING));
    }

    @Test
    void otherStatesAreLeftUntouched() {
        EnumSet<ExecutionState> resolved =
                EnumSet.of(
                        ExecutionState.DEPLOYING, ExecutionState.RUNNING, ExecutionState.CANCELING);
        for (ExecutionState state : EnumSet.complementOf(resolved)) {
            Assertions.assertNull(
                    CoordinatorService.resolveLostMemberState(state),
                    "member loss must not touch a vertex in state " + state);
        }
        Assertions.assertNull(CoordinatorService.resolveLostMemberState(null));
    }
}

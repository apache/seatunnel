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

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.execution.ExecutionState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Optional;

/**
 * Covers {@link CoordinatorService#resolveLostMemberState(ExecutionState, JobStatus)}, the decision
 * applied to every task vertex whose worker leaves the cluster: which terminal state the master
 * assigns locally now that the worker can no longer report one itself.
 *
 * <p>Guards the regression fixed alongside {@code
 * SplitClusterFaultToleranceIT#testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck}: while
 * the user is cancelling a job, a vertex on the lost worker must resolve to CANCELED, not FAILED,
 * whether the cancel already reached it (CANCELING) or not yet (DEPLOYING, RUNNING), so a
 * user-cancelled job is not reported as failed just because a worker was lost while the cancel was
 * in flight. In every other job status the same vertices keep resolving to FAILED, including a
 * CANCELING vertex whose cancel was started by the engine (master-switch reschedule, checkpoint
 * error, failing sibling), so a job the user never cancelled cannot end CANCELED with its "node
 * offline" reason dropped. Vertices in any other state are left untouched.
 */
class CoordinatorServiceLostMemberResolutionTest {

    private static final EnumSet<ExecutionState> STATES_WITH_WORK_ON_WORKER =
            EnumSet.of(ExecutionState.DEPLOYING, ExecutionState.RUNNING, ExecutionState.CANCELING);

    @Test
    void cancelingVertexOfUserCancelledJobResolvesToCanceled() {
        Assertions.assertEquals(
                Optional.of(ExecutionState.CANCELED),
                CoordinatorService.resolveLostMemberState(
                        ExecutionState.CANCELING, JobStatus.CANCELING));
    }

    /**
     * SubPlan cancels its vertices one by one, so siblings the cancel loop has not reached yet are
     * still DEPLOYING or RUNNING when the member is removed. They must not flip the user-cancelled
     * job to FAILED.
     */
    @Test
    void notYetCancelledVerticesOfUserCancelledJobResolveToCanceled() {
        Assertions.assertEquals(
                Optional.of(ExecutionState.CANCELED),
                CoordinatorService.resolveLostMemberState(
                        ExecutionState.DEPLOYING, JobStatus.CANCELING));
        Assertions.assertEquals(
                Optional.of(ExecutionState.CANCELED),
                CoordinatorService.resolveLostMemberState(
                        ExecutionState.RUNNING, JobStatus.CANCELING));
    }

    /**
     * A vertex can be CANCELING without any user cancel: the engine cancels tasks itself when it
     * reschedules a pipeline after a master switch, on a checkpoint error, or when a sibling
     * failed. The job status is then anything but CANCELING and the member loss must stay a
     * failure.
     */
    @Test
    void engineInitiatedCancelStillResolvesToFailed() {
        for (JobStatus jobStatus : EnumSet.complementOf(EnumSet.of(JobStatus.CANCELING))) {
            Assertions.assertEquals(
                    Optional.of(ExecutionState.FAILED),
                    CoordinatorService.resolveLostMemberState(ExecutionState.CANCELING, jobStatus),
                    "a CANCELING vertex must fail on member loss when the job is " + jobStatus);
        }
        Assertions.assertEquals(
                Optional.of(ExecutionState.FAILED),
                CoordinatorService.resolveLostMemberState(ExecutionState.CANCELING, null));
    }

    @Test
    void deployingAndRunningVerticesStillResolveToFailed() {
        for (JobStatus jobStatus : EnumSet.complementOf(EnumSet.of(JobStatus.CANCELING))) {
            Assertions.assertEquals(
                    Optional.of(ExecutionState.FAILED),
                    CoordinatorService.resolveLostMemberState(ExecutionState.DEPLOYING, jobStatus),
                    "a DEPLOYING vertex must fail on member loss when the job is " + jobStatus);
            Assertions.assertEquals(
                    Optional.of(ExecutionState.FAILED),
                    CoordinatorService.resolveLostMemberState(ExecutionState.RUNNING, jobStatus),
                    "a RUNNING vertex must fail on member loss when the job is " + jobStatus);
        }
        Assertions.assertEquals(
                Optional.of(ExecutionState.FAILED),
                CoordinatorService.resolveLostMemberState(ExecutionState.DEPLOYING, null));
        Assertions.assertEquals(
                Optional.of(ExecutionState.FAILED),
                CoordinatorService.resolveLostMemberState(ExecutionState.RUNNING, null));
    }

    @Test
    void otherStatesAreLeftUntouched() {
        for (ExecutionState state : EnumSet.complementOf(STATES_WITH_WORK_ON_WORKER)) {
            for (JobStatus jobStatus : JobStatus.values()) {
                Assertions.assertEquals(
                        Optional.empty(),
                        CoordinatorService.resolveLostMemberState(state, jobStatus),
                        "member loss must not touch a vertex in state "
                                + state
                                + " when the job is "
                                + jobStatus);
            }
        }
        Assertions.assertEquals(
                Optional.empty(),
                CoordinatorService.resolveLostMemberState(null, JobStatus.CANCELING));
        Assertions.assertEquals(
                Optional.empty(), CoordinatorService.resolveLostMemberState(null, null));
    }
}

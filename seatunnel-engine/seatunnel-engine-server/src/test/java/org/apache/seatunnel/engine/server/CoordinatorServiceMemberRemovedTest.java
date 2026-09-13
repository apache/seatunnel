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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

/**
 * Covers the coordinator-side helpers of the graceful member-removal classification: the failure
 * payload shape kept for departed workers, the marker TTL rule shared with the restore path, and
 * when a consumed marker may be cleared during master failover.
 */
class CoordinatorServiceMemberRemovedTest {

    /**
     * The failure state built for a departed worker must keep wrapping the offline message in a
     * {@code JobException}, matching the payload shape that existed before graceful classification.
     */
    @Test
    void shouldKeepThrowablePayloadForMemberRemovedFailureState() throws Exception {
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 2, 3L);
        Address address = new Address("127.0.0.1", 5801);

        TaskExecutionState taskExecutionState =
                CoordinatorService.buildMemberRemovedFailureState(taskGroupLocation, address);

        Assertions.assertEquals(ExecutionState.FAILED, taskExecutionState.getExecutionState());
        Assertions.assertTrue(taskExecutionState.getThrowableMsg().contains("JobException"));
        Assertions.assertTrue(
                taskExecutionState
                        .getThrowableMsg()
                        .contains(
                                CoordinatorService.buildMemberRemovedOfflineMessage(
                                        taskGroupLocation, address)));
    }

    /**
     * Markers exactly at the TTL edge on either side are still graceful; anything beyond it, or a
     * missing marker, is treated as an unproven removal.
     */
    @Test
    void shouldValidateGracefulMemberRemovalMarkerWithinTtl() {
        long now = System.currentTimeMillis();

        Assertions.assertTrue(CoordinatorService.isGracefulMemberRemovalMarkerValid(now, now));
        Assertions.assertTrue(
                CoordinatorService.isGracefulMemberRemovalMarkerValid(
                        now - Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS, now));
        Assertions.assertTrue(
                CoordinatorService.isGracefulMemberRemovalMarkerValid(
                        now + Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS, now));
        Assertions.assertFalse(
                CoordinatorService.isGracefulMemberRemovalMarkerValid(
                        now - Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS - 1, now));
        Assertions.assertFalse(
                CoordinatorService.isGracefulMemberRemovalMarkerValid(
                        now + Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS + 1, now));
        Assertions.assertFalse(CoordinatorService.isGracefulMemberRemovalMarkerValid(null, now));
    }

    /**
     * Ensures master failover retains the marker until its TTL rather than racing scheduling: a
     * restored job is queued first and its vertices inspect the marker only after the recovery
     * future completes, so clearing it during restore would misclassify the same departure.
     */
    @Test
    void shouldRetainGracefulMemberRemovalMarkerDuringMasterSwitchRecovery() {
        Assertions.assertFalse(
                CoordinatorService.canClearGracefulMemberRemovalMarker(1L, true, false));
        Assertions.assertFalse(
                CoordinatorService.canClearGracefulMemberRemovalMarker(1L, false, true));
        Assertions.assertTrue(
                CoordinatorService.canClearGracefulMemberRemovalMarker(1L, false, false));
        Assertions.assertFalse(
                CoordinatorService.canClearGracefulMemberRemovalMarker(null, false, false));
    }
}

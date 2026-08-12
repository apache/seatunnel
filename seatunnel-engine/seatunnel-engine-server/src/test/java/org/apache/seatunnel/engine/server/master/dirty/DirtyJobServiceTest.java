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

package org.apache.seatunnel.engine.server.master.dirty;

import org.apache.seatunnel.engine.common.job.DirtyJobState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Protects execution-owner isolation for delayed dirty-job state processors. */
class DirtyJobServiceTest {

    @Test
    void shouldRejectUnknownOrPreviousExecutionOwner() {
        DirtyJobState state = DirtyJobState.create(1L, 200L, 2, 10, 600_000L, 0L);

        Assertions.assertTrue(DirtyJobService.isOwnedBy(state, 200L));
        Assertions.assertFalse(DirtyJobService.isOwnedBy(state, 100L));
        Assertions.assertFalse(DirtyJobService.isOwnedBy(state, 0L));
    }

    @Test
    void shouldNotReplaceCurrentIncompleteOwnerWithStaleRetry() {
        Map<Long, Long> owners = new ConcurrentHashMap<>();
        Map<Long, Long> incompleteOwners = new ConcurrentHashMap<>();
        DirtyJobService.registerLocalOwner(owners, incompleteOwners, 1L, 200L, false);
        Assertions.assertTrue(
                DirtyJobService.registerLocalIncompleteOwner(owners, incompleteOwners, 1L, 200L));

        Assertions.assertFalse(
                DirtyJobService.registerLocalIncompleteOwner(owners, incompleteOwners, 1L, 100L));
        Assertions.assertEquals(200L, incompleteOwners.get(1L));
    }

    @Test
    void shouldRetainEarlyIncompleteEvidenceForSameRestoredExecution() {
        Map<Long, Long> owners = new ConcurrentHashMap<>();
        Map<Long, Long> incompleteOwners = new ConcurrentHashMap<>();
        Assertions.assertTrue(
                DirtyJobService.registerLocalIncompleteOwner(owners, incompleteOwners, 1L, 200L));

        DirtyJobService.registerLocalOwner(owners, incompleteOwners, 1L, 200L, true);

        Assertions.assertEquals(200L, incompleteOwners.get(1L));
    }

    @Test
    void shouldRemoveIncompleteEvidenceFromPreviousExecution() {
        Map<Long, Long> owners = new ConcurrentHashMap<>();
        Map<Long, Long> incompleteOwners = new ConcurrentHashMap<>();
        incompleteOwners.put(1L, 100L);

        DirtyJobService.registerLocalOwner(owners, incompleteOwners, 1L, 200L, true);

        Assertions.assertFalse(incompleteOwners.containsKey(1L));
    }
}

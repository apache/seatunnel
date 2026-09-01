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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Tests the graceful member-removal classification in {@link PhysicalVertex}. */
public class PhysicalVertexTest {

    /**
     * Only the structured graceful-member-removal classification should downgrade the failure log
     * level.
     */
    @Test
    public void shouldWarnOnlyForGracefulMemberRemovalFailureType() {
        Assertions.assertTrue(PhysicalVertex.shouldLogFailureAsWarn(true));
        Assertions.assertFalse(PhysicalVertex.shouldLogFailureAsWarn(false));
    }

    /**
     * Reproduces the race interleaving reported in review: a genuine task failure (RPC-reported,
     * non-graceful) records its classification first, then the coordinator's node-offline
     * classification for the same vertex lands afterwards. Before the message and the flag were
     * paired in one first-write-wins holder, the late graceful write silently re-tagged the
     * already-recorded genuine failure, so a real fault was logged at warn instead of error. The
     * recorded pair must stay the genuine failure's own classification.
     */
    @Test
    public void shouldNotRetagRecordedGenuineFailureAsGraceful() {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        PhysicalVertex.recordFailureClassification(slot, "genuine task failure", false);
        PhysicalVertex.recordFailureClassification(slot, "deployed node offline", true);
        Assertions.assertEquals("genuine task failure", slot.get().getErrorMessage());
        Assertions.assertFalse(slot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * The reverse interleaving: when the node-offline classification wins the first write, a later
     * genuine failure report must not strip the graceful flag from the recorded offline failure,
     * matching the pre-existing first-write-wins semantics of the recorded failure message.
     */
    @Test
    public void shouldNotStripGracefulFlagFromRecordedOfflineFailure() {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        PhysicalVertex.recordFailureClassification(slot, "deployed node offline", true);
        PhysicalVertex.recordFailureClassification(slot, "genuine task failure", false);
        Assertions.assertEquals("deployed node offline", slot.get().getErrorMessage());
        Assertions.assertTrue(slot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * A {@code null} message must never claim the classification slot, so a later caller that
     * actually carries a failure message still wins the recorded classification. This preserves the
     * pre-existing behavior where a message-less state report left the error slot claimable.
     */
    @Test
    public void shouldIgnoreNullMessageAndKeepSlotClaimable() {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        PhysicalVertex.recordFailureClassification(slot, null, true);
        Assertions.assertNull(slot.get());
        PhysicalVertex.recordFailureClassification(slot, "genuine task failure", false);
        Assertions.assertEquals("genuine task failure", slot.get().getErrorMessage());
        Assertions.assertFalse(slot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * A master failover restores task state before its member-removed callback can run. A fresh
     * marker must therefore classify the discovered missing worker as graceful in that path too.
     */
    @Test
    public void shouldClassifyMissingWorkerDuringMasterFailoverAsGraceful() throws Exception {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 2, 3L);
        Address lostAddress = new Address("127.0.0.1", 5801);
        long nowMillis = 100_000L;

        PhysicalVertex.recordMemberRemovedFailure(
                slot, taskGroupLocation, lostAddress, nowMillis, nowMillis);

        Assertions.assertEquals(
                "The taskGroup("
                        + taskGroupLocation
                        + ") deployed node("
                        + lostAddress
                        + ") offline",
                slot.get().getErrorMessage());
        Assertions.assertTrue(slot.get().isGracefulMemberRemovalFailure());

        AtomicReference<PhysicalVertex.FailureClassification> staleSlot = new AtomicReference<>();
        PhysicalVertex.recordMemberRemovedFailure(
                staleSlot,
                taskGroupLocation,
                lostAddress,
                nowMillis - Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS - 1,
                nowMillis);
        Assertions.assertFalse(staleSlot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * Concurrency invariant for the paired holder: two racing writers (a genuine failure and a
     * graceful node-offline classification) may win the slot in either order, but the recorded flag
     * must always belong to the recorded message. The assertion is order-independent, so the test
     * stays deterministic while still exercising real cross-thread interleavings.
     */
    @Test
    public void shouldKeepMessageAndFlagPairedUnderConcurrentWriters() throws Exception {
        for (int i = 0; i < 100; i++) {
            AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
            CountDownLatch startLatch = new CountDownLatch(1);
            Thread genuineFailureWriter =
                    new Thread(
                            () -> {
                                awaitStart(startLatch);
                                PhysicalVertex.recordFailureClassification(
                                        slot, "genuine task failure", false);
                            });
            Thread offlineClassificationWriter =
                    new Thread(
                            () -> {
                                awaitStart(startLatch);
                                PhysicalVertex.recordFailureClassification(
                                        slot, "deployed node offline", true);
                            });
            genuineFailureWriter.start();
            offlineClassificationWriter.start();
            startLatch.countDown();
            genuineFailureWriter.join();
            offlineClassificationWriter.join();
            PhysicalVertex.FailureClassification recorded = slot.get();
            Assertions.assertNotNull(recorded);
            Assertions.assertEquals(
                    "deployed node offline".equals(recorded.getErrorMessage()),
                    recorded.isGracefulMemberRemovalFailure());
        }
    }

    /**
     * Releases the writer threads at the same moment to maximize the chance of a real interleaving
     * between the two classification writes.
     */
    private static void awaitStart(CountDownLatch startLatch) {
        try {
            startLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}

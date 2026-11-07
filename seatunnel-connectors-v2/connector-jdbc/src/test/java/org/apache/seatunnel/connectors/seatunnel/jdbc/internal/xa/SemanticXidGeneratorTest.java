/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.util.Arrays;
import java.util.Collections;

class SemanticXidGeneratorTest {
    private SemanticXidGenerator xidGenerator;

    @BeforeEach
    void before() {
        xidGenerator = new SemanticXidGenerator();
        xidGenerator.open();
    }

    @Test
    void testBelongsToSubtask() {
        JobContext uuidJobContext = new JobContext();
        check(uuidJobContext);
        JobContext longJobContext = new JobContext(Long.MIN_VALUE);
        check(longJobContext);
    }

    @Test
    void testRestoreProtectsAllCheckpointXidsAndKeepsOwnershipAcrossAttempts() {
        JobContext job = new JobContext(1L);
        DefaultSinkWriterContext context = new DefaultSinkWriterContext(0, 1);
        Xid first = xidGenerator.generateXid(job, context, 100000000000L);
        Xid second = xidGenerator.generateXid(job, context, 100000000001L);
        Xid orphan = xidGenerator.generateXid(job, context, 100000000002L);
        SemanticXidGenerator restored = new SemanticXidGenerator();
        restored.open();
        Assertions.assertEquals(100000000001L, restored.restoreState(Arrays.asList(first, second)));
        Assertions.assertFalse(restored.canRollback(first, job, context));
        Assertions.assertFalse(restored.canRollback(second, job, context));
        Assertions.assertTrue(restored.canRollback(orphan, job, context));
        Xid nextAttempt = restored.generateXid(job, context, 100000000003L);
        SemanticXidGenerator restoredAgain = new SemanticXidGenerator();
        restoredAgain.open();
        restoredAgain.restoreState(Arrays.asList(first, second));
        Assertions.assertTrue(restoredAgain.canRollback(nextAttempt, job, context));
        Assertions.assertArrayEquals(first.getBranchQualifier(), nextAttempt.getBranchQualifier());
    }

    @Test
    void testRestoreMatchesCompleteOwnerTuplesAcrossRescaleAndNewJob() {
        JobContext oldJob = new JobContext(1L);
        JobContext newJob = new JobContext(2L);
        DefaultSinkWriterContext oldContext = new DefaultSinkWriterContext(1, 2);
        DefaultSinkWriterContext newContext = new DefaultSinkWriterContext(0, 1);
        Xid saved = xidGenerator.generateXid(oldJob, oldContext, 10);
        Xid orphan = xidGenerator.generateXid(oldJob, oldContext, 11);
        SemanticXidGenerator other = new SemanticXidGenerator();
        other.open();
        Xid otherSaved = other.generateXid(oldJob, newContext, 12);
        Xid crossTuple =
                new XidImpl(
                        saved.getFormatId(),
                        otherSaved.getGlobalTransactionId(),
                        saved.getBranchQualifier());
        SemanticXidGenerator restored = new SemanticXidGenerator();
        restored.open();
        restored.restoreState(Arrays.asList(saved, otherSaved));
        Assertions.assertTrue(restored.canRollback(orphan, newJob, newContext));
        Assertions.assertFalse(restored.canRollback(crossTuple, newJob, newContext));
        Assertions.assertFalse(restored.canRollback(otherSaved, newJob, newContext));
        Assertions.assertTrue(
                restored.canRollback(
                        restored.generateXid(newJob, newContext, 13), newJob, newContext));
    }

    @Test
    void testMalformedRecoveredXidsAreNeverOwnedAndMalformedStateFails() {
        JobContext job = new JobContext(1L);
        DefaultSinkWriterContext context = new DefaultSinkWriterContext(0, 1);
        Xid valid = xidGenerator.generateXid(job, context, 10);
        Xid[] invalid = {
            new XidImpl(201, new byte[1], new byte[4]),
            new XidImpl(201, new byte[44], new byte[1]),
            new XidImpl(202, valid.getGlobalTransactionId(), valid.getBranchQualifier())
        };
        for (Xid xid : invalid) {
            Assertions.assertFalse(xidGenerator.canRollback(xid, job, context));
            Assertions.assertFalse(xidGenerator.belongsToSubtask(xid, job, context));
            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> xidGenerator.restoreState(Collections.singletonList(xid)));
        }
    }

    void check(JobContext jobContext) {
        DefaultSinkWriterContext dc1 = new DefaultSinkWriterContext(Integer.MAX_VALUE, 1);
        Xid xid1 = xidGenerator.generateXid(jobContext, dc1, System.currentTimeMillis());
        Assertions.assertTrue(xidGenerator.belongsToSubtask(xid1, jobContext, dc1));
        Assertions.assertFalse(
                xidGenerator.belongsToSubtask(
                        xid1, jobContext, new DefaultSinkWriterContext(2, 1)));
        Assertions.assertFalse(xidGenerator.belongsToSubtask(xid1, new JobContext(), dc1));
    }
}

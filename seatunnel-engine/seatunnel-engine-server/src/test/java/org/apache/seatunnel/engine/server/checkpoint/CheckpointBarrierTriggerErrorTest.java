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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import com.hazelcast.internal.serialization.Data;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class CheckpointBarrierTriggerErrorTest extends AbstractSeaTunnelServerTest {

    private static final String CONF_PATH =
            "stream_fake_to_console_checkpoint_barrier_trigger_error.conf";
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    @Test
    public void testCheckpointBarrierTriggerError()
            throws NoSuchFieldException, IllegalAccessException {
        COUNTER.set(0);
        long jobId = System.currentTimeMillis();
        startJob(jobId, CONF_PATH);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.RUNNING));

        CheckpointManager spiedCheckpointManager = spy(getCheckpointManager(jobId));
        doAnswer(this::mockException)
                .when(spiedCheckpointManager)
                .sendOperationToMemberNode(Mockito.any(CheckpointBarrierTriggerOperation.class));
        setCheckpointManager(spiedCheckpointManager);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    server.getCoordinatorService().getJobStatus(jobId),
                                    JobStatus.FAILED);
                        });
    }

    private void startJob(Long jobid, String path) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobid.toString(), jobid);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobid,
                        "Test",
                        false,
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(jobid, data, jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }

    private Object mockException(InvocationOnMock invocation) throws Throwable {
        if (COUNTER.incrementAndGet() == 1) {
            throw new RuntimeException(
                    "An exception occurred while sending CheckpointBarrierTriggerOperation.");
        }
        return invocation.callRealMethod();
    }

    private CheckpointManager getCheckpointManager(Long jobId)
            throws NoSuchFieldException, IllegalAccessException {
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        Field checkpointManagerField = JobMaster.class.getDeclaredField("checkpointManager");
        checkpointManagerField.setAccessible(true);
        return (CheckpointManager) checkpointManagerField.get(jobMaster);
    }

    private void setCheckpointManager(CheckpointManager checkpointManager)
            throws NoSuchFieldException, IllegalAccessException {
        CheckpointCoordinator checkpointCoordinator = checkpointManager.getCheckpointCoordinator(1);
        Field checkpointManagerField =
                CheckpointCoordinator.class.getDeclaredField("checkpointManager");
        checkpointManagerField.setAccessible(true);
        checkpointManagerField.set(checkpointCoordinator, checkpointManager);
    }
}

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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.concurrent.Executors;

import static org.apache.seatunnel.engine.common.config.server.QueueType.BLOCKINGQUEUE;

class StateTransitionCleanupTest extends AbstractSeaTunnelServerTest {

    @Test
    void testSubPlanUsesLocalStateWhenDistributedPipelineStateAlreadyCleaned() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        SubPlan subPlan = planWithStateMaps.physicalPlan.getPipelineList().get(0);
        PipelineLocation pipelineLocation = subPlan.getPipelineLocation();

        planWithStateMaps.runningJobState.remove(pipelineLocation);
        planWithStateMaps.runningJobStateTimestamps.remove(pipelineLocation);

        subPlan.updatePipelineState(PipelineStatus.SCHEDULED);

        Assertions.assertEquals(PipelineStatus.SCHEDULED, subPlan.getPipelineState());
        Assertions.assertNull(planWithStateMaps.runningJobState.get(pipelineLocation));
    }

    @Test
    void testPhysicalPlanCompletesTerminalFutureWhenDistributedJobStateAlreadyCleaned()
            throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture<JobResult>
                jobEndFuture =
                        new org.apache.seatunnel.engine.common.utils.concurrent
                                .CompletableFuture<>();
        setField(planWithStateMaps.physicalPlan, "jobEndFuture", jobEndFuture);
        setBooleanField(planWithStateMaps.physicalPlan, "isRunning", true);

        planWithStateMaps.runningJobState.remove(jobId);
        planWithStateMaps.runningJobStateTimestamps.remove(jobId);

        planWithStateMaps.physicalPlan.updateJobState(JobStatus.FAILED);

        Assertions.assertTrue(jobEndFuture.isDone());
        Assertions.assertEquals(JobStatus.FAILED, jobEndFuture.get().getStatus());
        Assertions.assertEquals(JobStatus.FAILED, planWithStateMaps.physicalPlan.getJobStatus());
        Assertions.assertNull(planWithStateMaps.runningJobState.get(jobId));
    }

    private PlanWithStateMaps createPhysicalPlan(long jobId) throws MalformedURLException {
        JobContext jobContext = new JobContext(jobId);
        jobContext.setJobMode(JobMode.BATCH);
        JobConfig config = new JobConfig();
        config.setName("cleanup-test");
        config.setJobContext(jobContext);
        LogicalDag logicalDag = TestUtils.getTestLogicalDag(jobContext, config);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "CleanupTest",
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("cleanupRunningJobState-" + jobId);
        IMap<Object, Long[]> runningJobStateTimestamps =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap("cleanupRunningJobStateTimestamps-" + jobId);

        PhysicalPlan physicalPlan =
                PlanUtils.fromLogicalDAG(
                                logicalDag,
                                nodeEngine,
                                jobImmutableInformation,
                                System.currentTimeMillis(),
                                Executors.newCachedThreadPool(),
                                server.getClassLoaderService(),
                                instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME),
                                runningJobState,
                                runningJobStateTimestamps,
                                BLOCKINGQUEUE,
                                new EngineConfig())
                        .f0();

        return new PlanWithStateMaps(physicalPlan, runningJobState, runningJobStateTimestamps);
    }

    private void setBooleanField(Object target, String fieldName, boolean value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setBoolean(target, value);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class PlanWithStateMaps {
        private final PhysicalPlan physicalPlan;
        private final IMap<Object, Object> runningJobState;
        private final IMap<Object, Long[]> runningJobStateTimestamps;

        private PlanWithStateMaps(
                PhysicalPlan physicalPlan,
                IMap<Object, Object> runningJobState,
                IMap<Object, Long[]> runningJobStateTimestamps) {
            this.physicalPlan = physicalPlan;
            this.runningJobState = runningJobState;
            this.runningJobStateTimestamps = runningJobStateTimestamps;
        }
    }
}

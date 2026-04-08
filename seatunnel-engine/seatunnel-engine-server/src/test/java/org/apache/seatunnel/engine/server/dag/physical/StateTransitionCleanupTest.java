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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.execution.ExecutionState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import com.hazelcast.map.IMap;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.concurrent.Executors;

import static org.apache.seatunnel.engine.common.config.server.QueueType.BLOCKINGQUEUE;
import static org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService.SKIP_CHECK_JAR;

@SetEnvironmentVariable(key = SKIP_CHECK_JAR, value = "true")
class StateTransitionCleanupTest extends AbstractSeaTunnelServerTest {

    @Test
    void testPhysicalVertexIgnoresLateTransitionWhenTaskStateAlreadyTerminal() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        PhysicalVertex physicalVertex =
                planWithStateMaps
                        .physicalPlan
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0);

        planWithStateMaps.runningJobState.put(
                physicalVertex.getTaskGroupLocation(), ExecutionState.FAILED);

        physicalVertex.updateTaskState(ExecutionState.CANCELING);

        Assertions.assertEquals(
                ExecutionState.FAILED,
                planWithStateMaps.runningJobState.get(physicalVertex.getTaskGroupLocation()));
    }

    @Test
    void testSubPlanIgnoresLateTransitionWhenPipelineStateAlreadyTerminal() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        SubPlan subPlan = planWithStateMaps.physicalPlan.getPipelineList().get(0);
        PipelineLocation pipelineLocation = subPlan.getPipelineLocation();

        planWithStateMaps.runningJobState.put(pipelineLocation, PipelineStatus.FAILED);

        subPlan.updatePipelineState(PipelineStatus.CANCELING);

        Assertions.assertEquals(
                PipelineStatus.FAILED, planWithStateMaps.runningJobState.get(pipelineLocation));
    }

    @Test
    void testPhysicalPlanIgnoresLateTransitionWhenJobStateAlreadyTerminal() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        planWithStateMaps.runningJobState.put(jobId, JobStatus.FAILED);

        planWithStateMaps.physicalPlan.updateJobState(JobStatus.CANCELING);

        Assertions.assertEquals(JobStatus.FAILED, planWithStateMaps.runningJobState.get(jobId));
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

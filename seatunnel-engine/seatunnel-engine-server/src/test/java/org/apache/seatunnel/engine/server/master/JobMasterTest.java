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

package org.apache.seatunnel.engine.server.master;

import static org.awaitility.Awaitility.await;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.core.job.RunningJobInfo;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * JobMaster Tester.
 */
@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JobMasterTest extends AbstractSeaTunnelServerTest {
    private static Long JOB_ID;

    /**
     * IMap key is jobId and value is a Tuple2
     * Tuple2 key is JobMaster init timestamp and value is the jobImmutableInformation which is sent by client when submit job
     * <p>
     * This IMap is used to recovery runningJobInfoIMap in JobMaster when a new master node active
     */
    private IMap<Long, RunningJobInfo> runningJobInfoIMap;

    /**
     * IMap key is one of jobId {@link org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and
     * {@link org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     * <p>
     * The value of IMap is one of {@link JobStatus} {@link PipelineStatus}
     * {@link org.apache.seatunnel.engine.server.execution.ExecutionState}
     * <p>
     * This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node active
     */
    IMap<Object, Object> runningJobStateIMap;

    /**
     * IMap key is one of jobId {@link org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and
     * {@link org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     * <p>
     * The value of IMap is one of {@link org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan} stateTimestamps
     * {@link org.apache.seatunnel.engine.server.dag.physical.SubPlan} stateTimestamps
     * {@link org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex} stateTimestamps
     * <p>
     * This IMap is used to recovery runningJobStateTimestampsIMap in JobMaster when a new master node active
     */
    IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * IMap key is {@link PipelineLocation}
     * <p>
     * The value of IMap is map of {@link TaskGroupLocation} and the {@link SlotProfile} it used.
     * <p>
     * This IMap is used to recovery ownedSlotProfilesIMap in JobMaster when a new master node active
     */
    private IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    @BeforeAll
    public void before() {
        super.before();
        JOB_ID = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
    }

    @Test
    public void testHandleCheckpointTimeout() throws Exception {
        LogicalDag testLogicalDag =
            TestUtils.createTestLogicalPlan("stream_fakesource_to_file.conf", "test_clear_coordinator_service", JOB_ID);

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(JOB_ID,
            nodeEngine.getSerializationService().toData(testLogicalDag), testLogicalDag.getJobConfig(),
            Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
            server.getCoordinatorService().submitJob(JOB_ID, data);
        voidPassiveCompletableFuture.join();

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(JOB_ID);

        // waiting for job status turn to running
        await().atMost(120000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus()));

        // call checkpoint timeout
        jobMaster.handleCheckpointTimeout(1);

        // Because handleCheckpointTimeout is an async method, so we need sleep 5s to waiting job status become running again
        Thread.sleep(5000);

        // test job still run
        await().atMost(120000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus()));

        PassiveCompletableFuture<JobStatus> jobMasterCompleteFuture = jobMaster.getJobMasterCompleteFuture();
        // cancel job
        jobMaster.cancelJob();

        // test job turn to complete
        await().atMost(120000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                jobMasterCompleteFuture.isDone() && JobStatus.CANCELED.equals(jobMasterCompleteFuture.get())));

        testIMapRemovedAfterJobComplete(jobMaster);
    }

    private void testIMapRemovedAfterJobComplete(JobMaster jobMaster) {
        runningJobInfoIMap = nodeEngine.getHazelcastInstance().getMap("runningJobInfo");
        runningJobStateIMap = nodeEngine.getHazelcastInstance().getMap("runningJobState");
        runningJobStateTimestampsIMap = nodeEngine.getHazelcastInstance().getMap("stateTimestamps");
        ownedSlotProfilesIMap = nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");

        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                Assertions.assertNull(runningJobInfoIMap.get(JOB_ID));
                Assertions.assertNull(runningJobStateIMap.get(JOB_ID));
                Assertions.assertNull(runningJobStateTimestampsIMap.get(JOB_ID));
                Assertions.assertNull(ownedSlotProfilesIMap.get(JOB_ID));

                jobMaster.getPhysicalPlan().getPipelineList().forEach(pipeline -> {
                    Assertions.assertNull(
                        runningJobStateIMap.get(pipeline.getPipelineLocation()));

                    Assertions.assertNull(
                        runningJobStateTimestampsIMap.get(pipeline.getPipelineLocation()));
                });
                jobMaster.getPhysicalPlan().getPipelineList().forEach(pipeline -> {
                    pipeline.getCoordinatorVertexList().forEach(coordinator -> {
                        Assertions.assertNull(
                            runningJobStateIMap.get(coordinator.getTaskGroupLocation()));

                        Assertions.assertNull(
                            runningJobStateTimestampsIMap.get(coordinator.getTaskGroupLocation()));
                    });

                    pipeline.getPhysicalVertexList().forEach(task -> {
                        Assertions.assertNull(
                            runningJobStateIMap.get(task.getTaskGroupLocation()));

                        Assertions.assertNull(
                            runningJobStateTimestampsIMap.get(task.getTaskGroupLocation()));
                    });
                });
            });
    }
}

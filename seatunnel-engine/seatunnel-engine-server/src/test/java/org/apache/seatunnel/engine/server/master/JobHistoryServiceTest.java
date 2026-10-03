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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStatusData;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobHistoryServiceTest extends AbstractSeaTunnelServerTest {

    private static final Long JOB_1 = System.currentTimeMillis() + 1L;
    private static final Long JOB_2 = System.currentTimeMillis() + 2L;
    private static final Long JOB_3 = System.currentTimeMillis() + 3L;

    @Test
    public void testlistJobState() throws Exception {
        startJob(JOB_1, "fake_to_console.conf");

        // waiting for JOB_1 status turn to RUNNING
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobStatusData> jobStatusData = listJob();
                            Optional<JobStatusData> job =
                                    jobStatusData.stream()
                                            .filter(jobStatus -> jobStatus.getJobId().equals(JOB_1))
                                            .findFirst();
                            Assertions.assertTrue(job.isPresent());
                            Assertions.assertEquals(JobStatus.RUNNING, job.get().getJobStatus());
                            Assertions.assertEquals("Test", job.get().getJobName());
                            Assertions.assertNotNull(job.get().getStartTime());
                            Assertions.assertNotNull(
                                    job.get().getStartTime() > job.get().getSubmitTime());
                        });

        // waiting for JOB_1 status turn to FINISHED
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobStatusData> jobStatusData = listJob();
                            Optional<JobStatusData> job =
                                    jobStatusData.stream()
                                            .filter(jobStatus -> jobStatus.getJobId().equals(JOB_1))
                                            .findFirst();
                            Assertions.assertTrue(job.isPresent());
                            Assertions.assertEquals(JobStatus.FINISHED, job.get().getJobStatus());
                            Assertions.assertEquals("Test", job.get().getJobName());
                            Assertions.assertNotNull(job.get().getStartTime());
                            Assertions.assertNotNull(job.get().getFinishTime());
                            Assertions.assertNotNull(
                                    job.get().getFinishTime() > job.get().getStartTime());
                        });

        startJob(JOB_2, "fake_to_console.conf");
        // waiting for JOB_2 status turn to FINISHED and JOB_2 status turn to RUNNING
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobStatusData> jobStatusData = listJob();
                            Optional<JobStatusData> job1 =
                                    jobStatusData.stream()
                                            .filter(jobStatus -> jobStatus.getJobId().equals(JOB_1))
                                            .findFirst();
                            Assertions.assertTrue(job1.isPresent());
                            Assertions.assertEquals(JobStatus.FINISHED, job1.get().getJobStatus());
                            Assertions.assertEquals("Test", job1.get().getJobName());
                            Assertions.assertNotNull(job1.get().getStartTime());
                            Assertions.assertNotNull(job1.get().getFinishTime());
                            Optional<JobStatusData> job2 =
                                    jobStatusData.stream()
                                            .filter(jobStatus -> jobStatus.getJobId().equals(JOB_2))
                                            .findFirst();
                            Assertions.assertTrue(job2.isPresent());
                            Assertions.assertEquals(JobStatus.RUNNING, job2.get().getJobStatus());
                            Assertions.assertEquals("Test", job2.get().getJobName());
                            Assertions.assertNotNull(job2.get().getStartTime());
                            Assertions.assertNotNull(
                                    job2.get().getStartTime() > job2.get().getSubmitTime());
                        });
    }

    @Test
    public void testGetJobStatus() throws Exception {
        startJob(JOB_3, "fake_to_console.conf");
        // waiting for JOB_3 status turn to RUNNING
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                        .getJobHistoryService()
                                                        .getJobDetailStateAsString(JOB_3)
                                                        .contains("TaskGroupLocation")
                                                && server.getCoordinatorService()
                                                        .getJobHistoryService()
                                                        .getJobDetailStateAsString(JOB_3)
                                                        .contains("RUNNING")));

        // waiting for job1 status turn to FINISHED
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                        .getJobHistoryService()
                                                        .getJobDetailStateAsString(JOB_3)
                                                        .contains("TaskGroupLocation")
                                                && server.getCoordinatorService()
                                                        .getJobHistoryService()
                                                        .getJobDetailStateAsString(JOB_3)
                                                        .contains("FINISHED")));
    }

    @Test
    public void testShutdownRemovesListeners() {
        JobHistoryService jobHistoryService = server.getCoordinatorService().getJobHistoryService();
        Assertions.assertNotNull(jobHistoryService);

        // Verify listener UUIDs were stored during construction
        UUID stateListenerId =
                (UUID)
                        ReflectionUtils.getField(jobHistoryService, "finishedJobStateListenerId")
                                .orElse(null);
        UUID metricsListenerId =
                (UUID)
                        ReflectionUtils.getField(jobHistoryService, "finishedJobMetricsListenerId")
                                .orElse(null);
        UUID dagInfoListenerId =
                (UUID)
                        ReflectionUtils.getField(jobHistoryService, "finishedJobDAGInfoListenerId")
                                .orElse(null);
        Assertions.assertNotNull(stateListenerId, "finishedJobStateListenerId should not be null");
        Assertions.assertNotNull(
                metricsListenerId, "finishedJobMetricsListenerId should not be null");
        Assertions.assertNotNull(
                dagInfoListenerId, "finishedJobDAGInfoListenerId should not be null");

        // Get the IMaps so we can verify listeners were removed
        IMap<?, ?> finishedJobStateImap =
                (IMap<?, ?>)
                        ReflectionUtils.getField(jobHistoryService, "finishedJobStateImap")
                                .orElse(null);
        IMap<?, ?> finishedJobMetricsImap =
                (IMap<?, ?>)
                        ReflectionUtils.getField(jobHistoryService, "finishedJobMetricsImap")
                                .orElse(null);
        IMap<?, ?> finishedJobDAGInfoImap =
                (IMap<?, ?>)
                        ReflectionUtils.getField(jobHistoryService, "finishedJobDAGInfoImap")
                                .orElse(null);
        Assertions.assertNotNull(finishedJobStateImap);
        Assertions.assertNotNull(finishedJobMetricsImap);
        Assertions.assertNotNull(finishedJobDAGInfoImap);

        // Call shutdown to remove the listeners
        jobHistoryService.shutdown();

        // Verify listeners were removed: removeEntryListener returns false when listener
        // has already been deregistered
        Assertions.assertFalse(
                finishedJobStateImap.removeEntryListener(stateListenerId),
                "finishedJobState listener should have been removed by shutdown()");
        Assertions.assertFalse(
                finishedJobMetricsImap.removeEntryListener(metricsListenerId),
                "finishedJobMetrics listener should have been removed by shutdown()");
        Assertions.assertFalse(
                finishedJobDAGInfoImap.removeEntryListener(dagInfoListenerId),
                "finishedJobDAGInfo listener should have been removed by shutdown()");
    }

    private void startJob(Long jobid, String path) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobid.toString(), jobid);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobid,
                        "Test",
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

    private List<JobStatusData> listJob() {
        String listAllJob = server.getCoordinatorService().getJobHistoryService().listAllJob();
        return JsonUtils.toList(listAllJob, JobStatusData.class);
    }
}

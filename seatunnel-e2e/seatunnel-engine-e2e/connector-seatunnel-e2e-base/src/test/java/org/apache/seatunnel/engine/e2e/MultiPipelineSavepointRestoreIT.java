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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * End-to-end regression coverage for the per-pipeline contract of a Zeta stop-with-savepoint.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A savepoint is not a single job-level
 * checkpoint: {@code CheckpointManager#triggerSavePointsAndWaitComplete} fans the request out to
 * one {@code CheckpointCoordinator} per pipeline, and every pipeline has to persist its own
 * savepoint before the job may reach {@code SAVEPOINT_DONE}. The restore side is equally
 * per-pipeline: {@code MultipleTableJobConfigParser} requires one checkpoint record per source
 * config and rejects the restore unless the record for source index {@code i} carries pipeline id
 * {@code i + 1}. A regression that stops the fan-out after the first pipeline, overwrites one
 * pipeline's savepoint with a reused id, or persists the records in the wrong pipeline order
 * therefore produces a savepoint that still looks successful ({@code SAVEPOINT_DONE}) but cannot be
 * restored at all. Because checkpoint state is deleted only for FINISHED/CANCELED jobs, the broken
 * savepoint remains the only recovery point the operator has.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointRestoreIT} and {@code
 * CheckpointRestoreWithStopIT} drive single-pipeline jobs; {@code SavepointBusySourceBarrierIT}
 * mentions the per-coordinator fan-out but owns exactly one pipeline; {@code
 * SavepointPreconditionRecoveryIT} races a savepoint against one pipeline's task deployment; {@code
 * CommittedMetricsIT} uses a two-pipeline job but only reads metrics and never takes a savepoint.
 * No existing test asserts that every pipeline of a multi-pipeline job ends up with its own,
 * correctly numbered savepoint, or that a restore resumes both pipelines from those records.
 *
 * <p>Kept as its own class instead of extending {@code SavepointRestoreIT}: that test starts one
 * class-scoped Testcontainers cluster and uses a single-pipeline fixture, while this contract needs
 * two independent pipelines in one job and a client-side restore of both.
 */
@Slf4j
public class MultiPipelineSavepointRestoreIT {

    private static final String CONF_FILE = "stream_fake_two_pipeline_savepoint.conf";
    private static final String JOB_NAME = "MultiPipelineSavepointRestoreIT";

    /** Two source/sink pairs in {@link #CONF_FILE} produce pipeline ids 1 and 2. */
    private static final Set<Integer> EXPECTED_PIPELINE_IDS =
            Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(1, 2)));

    private static final long CLUSTER_TIMEOUT_SECONDS = 30;
    private static final long STATE_TIMEOUT_SECONDS = 180;

    @Test
    public void testSavepointIsPersistedPerPipelineAndRestoredForBothPipelines() throws Exception {
        String testClusterName = "MultiPipelineSavepointRestoreIT_" + System.currentTimeMillis();
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy restoredJob = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint is irrelevant for this client-side contract and would only contend
        // for the fixed port with other test classes.
        clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            awaitClusterSize(node1, 2);

            Common.setDeployMode(DeployMode.CLIENT);
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();
            String confPath = TestUtils.getResource(CONF_FILE);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(JOB_NAME);
            ClientJobProxy originalJob =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, clusterConfig)
                            .execute();
            long jobId = originalJob.getJobId();
            awaitJobStatus(originalJob, JobStatus.RUNNING);

            // Both pipelines must have completed an ordinary checkpoint first. This proves the
            // checkpoint scheduler is live for the whole job, so a later missing savepoint cannot
            // be explained by a pipeline that never checkpointed at all.
            Map<Integer, JobPipelineCheckpointData> periodicBeforeSavepoint =
                    awaitCheckpoints(
                            jobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            Collections.emptyMap(),
                            CheckpointType.CHECKPOINT_TYPE,
                            "periodic checkpoints before savepoint");

            // The client call blocks until the savepoint resolves. Run it on a worker so a stuck
            // DOING_SAVEPOINT transition surfaces as a bounded failure instead of hanging the
            // suite.
            awaitSavepoint(engineClient, jobId);
            awaitJobStatus(originalJob, JobStatus.SAVEPOINT_DONE);

            // Core assertion of the fan-out: one savepoint per pipeline, each newer than that
            // pipeline's last periodic checkpoint. A regression that reuses an id or persists only
            // the first pipeline's state fails here.
            Map<Integer, JobPipelineCheckpointData> savepoints =
                    awaitCheckpoints(
                            jobClient,
                            jobId,
                            RestoreMode.SAVEPOINT,
                            toCheckpointIds(periodicBeforeSavepoint),
                            CheckpointType.SAVEPOINT_TYPE,
                            "savepoints after stop-with-savepoint");

            JobConfig restoreJobConfig = new JobConfig();
            restoreJobConfig.setName(JOB_NAME + "_restore");
            restoredJob =
                    engineClient
                            .restoreExecutionContext(
                                    confPath, restoreJobConfig, clusterConfig, jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId,
                    restoredJob.getJobId(),
                    "A savepoint restore must keep the source job id");
            awaitJobStatus(restoredJob, JobStatus.RUNNING);

            // Both restored pipelines have to continue their own checkpoint id sequence after the
            // savepoint id. If one pipeline was restored without its savepoint state, it either
            // never checkpoints again or restarts from id 1 and the assertion below catches it.
            awaitCheckpoints(
                    jobClient,
                    jobId,
                    RestoreMode.CHECKPOINT,
                    toCheckpointIds(savepoints),
                    CheckpointType.CHECKPOINT_TYPE,
                    "periodic checkpoints after restore");

            // The savepoints are the only durable recovery point of this job. New periodic
            // checkpoints must not overwrite or delete them, otherwise a second restore could not
            // use the documented latest savepoint.
            assertSavepointsPreserved(jobClient, jobId, savepoints);

            engineClient.cancelJob(jobId);
            awaitJobStatus(restoredJob, JobStatus.CANCELED);
            restoredJob = null;
        } finally {
            if (restoredJob != null) {
                try {
                    restoredJob.cancelJob();
                } catch (Throwable cancelFailure) {
                    log.warn(
                            "Could not cancel restored job {} during teardown",
                            restoredJob.getJobId());
                }
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    private static void awaitSavepoint(SeaTunnelClient engineClient, long jobId)
            throws InterruptedException, ExecutionException {
        CompletableFuture<Void> savepoint =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        try {
            savepoint.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(
                    "Stop-with-savepoint for job "
                            + jobId
                            + " did not complete within "
                            + STATE_TIMEOUT_SECONDS
                            + "s",
                    e);
        }
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Unexpected job status for job " + jobProxy.getJobId()));
    }

    /**
     * Waits until every expected pipeline has a checkpoint of {@code expectedType} whose id is
     * greater than the pipeline's entry in {@code minExclusiveIdByPipeline}, and returns the latest
     * such checkpoint per pipeline.
     */
    private static Map<Integer, JobPipelineCheckpointData> awaitCheckpoints(
            JobClient jobClient,
            long jobId,
            RestoreMode restoreMode,
            Map<Integer, Long> minExclusiveIdByPipeline,
            CheckpointType expectedType,
            String description) {
        AtomicReference<Map<Integer, JobPipelineCheckpointData>> latest = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Map<Integer, JobPipelineCheckpointData> byPipeline =
                                    indexByPipeline(
                                            jobClient.getCheckpointData(jobId, restoreMode));
                            Assertions.assertEquals(
                                    EXPECTED_PIPELINE_IDS,
                                    byPipeline.keySet(),
                                    "Expected one "
                                            + description
                                            + " per pipeline for job "
                                            + jobId
                                            + ", but got "
                                            + byPipeline.values());
                            for (int pipelineId : EXPECTED_PIPELINE_IDS) {
                                JobPipelineCheckpointData checkpoint = byPipeline.get(pipelineId);
                                long minExclusiveId =
                                        minExclusiveIdByPipeline.getOrDefault(pipelineId, 0L);
                                Assertions.assertEquals(
                                        expectedType,
                                        checkpoint.getCheckpointType(),
                                        "Unexpected checkpoint type for "
                                                + description
                                                + " of pipeline "
                                                + pipelineId
                                                + ": "
                                                + checkpoint);
                                Assertions.assertTrue(
                                        checkpoint.getCheckpointId() > minExclusiveId,
                                        "Waiting for a "
                                                + description
                                                + " above id "
                                                + minExclusiveId
                                                + " for pipeline "
                                                + pipelineId
                                                + ", but the latest is "
                                                + checkpoint.getCheckpointId());
                            }
                            latest.set(byPipeline);
                        });
        return latest.get();
    }

    private static void assertSavepointsPreserved(
            JobClient jobClient,
            long jobId,
            Map<Integer, JobPipelineCheckpointData> expectedSavepoints) {
        Map<Integer, JobPipelineCheckpointData> currentSavepoints =
                indexByPipeline(jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT));
        Assertions.assertEquals(
                EXPECTED_PIPELINE_IDS,
                currentSavepoints.keySet(),
                "Every pipeline's savepoint must stay selectable after restore: "
                        + currentSavepoints.values());
        for (int pipelineId : EXPECTED_PIPELINE_IDS) {
            Assertions.assertEquals(
                    expectedSavepoints.get(pipelineId).getCheckpointId(),
                    currentSavepoints.get(pipelineId).getCheckpointId(),
                    "The restored pipeline "
                            + pipelineId
                            + " must not overwrite its savepoint checkpoint id");
        }
    }

    private static Map<Integer, JobPipelineCheckpointData> indexByPipeline(
            List<JobPipelineCheckpointData> checkpoints) {
        return checkpoints.stream()
                .collect(
                        Collectors.toMap(
                                JobPipelineCheckpointData::getPipelineId,
                                Function.identity(),
                                (left, right) ->
                                        left.getCheckpointId() >= right.getCheckpointId()
                                                ? left
                                                : right,
                                HashMap::new));
    }

    private static Map<Integer, Long> toCheckpointIds(
            Map<Integer, JobPipelineCheckpointData> checkpoints) {
        return checkpoints.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().getCheckpointId(),
                                (left, right) -> Math.max(left, right),
                                HashMap::new));
    }
}

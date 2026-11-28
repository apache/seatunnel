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
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.e2e.storagefail.FailingCheckpointStorage;
import org.apache.seatunnel.engine.e2e.storagefail.FailingCheckpointStorageFactory;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Regression guard for the engine's handling of a checkpoint that is fully acknowledged by every
 * task but cannot be written to checkpoint storage.
 *
 * <p>A checkpoint storage outage (an S3/HDFS request that fails, a full disk, a revoked credential)
 * hits the engine after the barrier has already traversed the whole pipeline. At that point the
 * engine owns a superstep of task state that it cannot persist, and the following invariants must
 * hold:
 *
 * <ul>
 *   <li>the checkpoint must be reported FAILED with the storage error, never COMPLETED, because a
 *       checkpoint that was not written cannot be used for recovery;
 *   <li>nothing of the rejected checkpoint may remain readable in storage, otherwise a later
 *       restore could pick up a recovery point that does not exist;
 *   <li>the checkpoint error must restart the pipeline within the job's retry budget instead of
 *       failing the streaming job, and the restored run must keep checkpointing with a fresh,
 *       higher id (the checkpoint id counter is part of the persisted engine contract);
 *   <li>once storage accepts writes again the pipeline must persist a real checkpoint that the
 *       client can select for {@code RestoreMode.CHECKPOINT}, and the job must still cancel cleanly
 *       and release its checkpoint data.
 * </ul>
 *
 * <p>Existing coverage stops before the storage boundary: {@code CheckpointStorageTest} only writes
 * and reads files, {@code CheckpointExpiryPipelineScopeIT} expires a barrier that never reaches the
 * sink, {@code SavepointSnapshotFailureIT} makes the sink reject its snapshot, and no E2E drives a
 * checkpoint that every task acknowledged through a failing {@link
 * CheckpointStorage#storeCheckPoint}. This test injects that failure with a test-only storage
 * plugin, so the engine path from "checkpoint complete" to "checkpoint durable" is exercised end to
 * end.
 */
@Slf4j
public class CheckpointStorageWriteFailureIT {

    private static final String CONF_FILE =
            "checkpoint-storage-failure/stream_fake_to_console_checkpoint_storage_failure.conf";

    /** Number of checkpoint writes the storage rejects before it starts accepting them. */
    private static final int INJECTED_STORE_FAILURES = 2;

    /** Single pipeline of the fixture job. */
    private static final int PIPELINE_ID = 1;

    private static final int HISTORY_LIMIT = 200;

    private static final long STATE_TIMEOUT_SECONDS = 120;

    @Test
    public void testCheckpointStorageFailureRestoresPipelineAndRecovers() throws Exception {
        String clusterName = "CheckpointStorageWriteFailureIT_" + System.currentTimeMillis();
        Path namespace = Files.createTempDirectory("zeta-checkpoint-storage-failure-");
        FailingCheckpointStorage.reset(INJECTED_STORE_FAILURES);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            CheckpointStorageConfig storageConfig =
                    seaTunnelConfig.getEngineConfig().getCheckpointConfig().getStorage();
            storageConfig.setStorage(FailingCheckpointStorageFactory.IDENTIFIER);
            storageConfig.getStoragePluginConfig().put("namespace", namespace.toString());
            // Keep every successful checkpoint for the duration of the test: retention-based
            // deletion would otherwise race the storage assertions on a slow CI runner.
            storageConfig.setMaxRetainedCheckpoints(50);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            SeaTunnelServer server = node.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = server.getCoordinatorService();
            CheckpointMonitorService checkpointMonitor = server.getCheckpointMonitorService();
            FailingCheckpointStorage checkpointStorage =
                    (FailingCheckpointStorage) server.getCheckpointService().getCheckpointStorage();

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("checkpoint-storage-write-failure");
            ClientJobExecutionEnvironment executionEnvironment =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(CONF_FILE), jobConfig, seaTunnelConfig);
            job = executionEnvironment.execute();
            long jobId = job.getJobId();

            awaitJobStatus(coordinatorService, jobId, JobStatus.RUNNING);

            // ---- Phase 1: the first two acknowledged checkpoints are rejected by storage.
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview pipeline =
                                        pipelines(checkpointMonitor, jobId).get(PIPELINE_ID);
                                Assertions.assertNotNull(
                                        pipeline, "the job's pipeline must be reported");
                                Assertions.assertTrue(
                                        pipeline.getCounts().getFailed() >= 1L,
                                        "a checkpoint whose storage write failed must be reported "
                                                + "as failed: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                                CheckpointInfo failed = pipeline.getLatestFailed();
                                Assertions.assertNotNull(
                                        failed,
                                        "the failed checkpoint must be recorded with its cause");
                                Assertions.assertEquals(
                                        CheckpointStatus.FAILED,
                                        failed.getStatus(),
                                        "a rejected checkpoint must not be COMPLETED");
                                Assertions.assertNotNull(
                                        failed.getFailureReason(),
                                        "the storage failure reason must be kept");
                                Assertions.assertTrue(
                                        FailingCheckpointStorage.rejectedCheckpointIds()
                                                .contains(failed.getCheckpointId()),
                                        "the failed checkpoint must be one whose storage write "
                                                + "was rejected, rejected="
                                                + FailingCheckpointStorage.rejectedCheckpointIds()
                                                + ", failed id="
                                                + failed.getCheckpointId());
                            });

            // ---- Phase 2: both rejections are followed by restores; a later write succeeds.
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview pipeline =
                                        pipelines(checkpointMonitor, jobId).get(PIPELINE_ID);
                                Assertions.assertNotNull(
                                        pipeline, "the job's pipeline must be reported");
                                Assertions.assertEquals(
                                        INJECTED_STORE_FAILURES,
                                        pipeline.getCounts().getFailed(),
                                        "exactly the injected storage failures must be recorded: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                                Assertions.assertTrue(
                                        pipeline.getCounts().getRestored()
                                                >= INJECTED_STORE_FAILURES,
                                        "every rejected checkpoint must restore the pipeline: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                                Assertions.assertTrue(
                                        pipeline.getCounts().getCompleted() >= 1L,
                                        "the restored pipeline must complete and persist a "
                                                + "checkpoint once storage accepts writes: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                            });
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    coordinatorService.getJobStatus(jobId),
                    "a transient checkpoint storage failure must not fail the streaming job");

            List<Long> rejectedIds =
                    new ArrayList<>(FailingCheckpointStorage.rejectedCheckpointIds());
            Assertions.assertEquals(
                    INJECTED_STORE_FAILURES,
                    rejectedIds.size(),
                    "the storage must have rejected exactly "
                            + INJECTED_STORE_FAILURES
                            + " writes");
            Assertions.assertTrue(
                    FailingCheckpointStorage.storeAttempts() > INJECTED_STORE_FAILURES,
                    "the engine must have retried the checkpoint write after the failures");

            // ---- Phase 3: the rejected checkpoints must not be selectable for recovery.
            // The job keeps checkpointing every second, so read storage first and then snapshot the
            // successful writes: every persisted id was written before that snapshot.
            List<PipelineState> persisted = getAllCheckpoints(checkpointStorage, jobId);
            Set<Long> persistedIds =
                    persisted.stream()
                            .map(PipelineState::getCheckpointId)
                            .collect(Collectors.toSet());
            List<Long> storedIds = new ArrayList<>(FailingCheckpointStorage.storedCheckpointIds());
            Assertions.assertFalse(
                    storedIds.isEmpty(), "no checkpoint was persisted after recovery");
            Assertions.assertFalse(persistedIds.isEmpty(), "storage holds no persisted checkpoint");
            Assertions.assertEquals(
                    persistedIds.size(), persisted.size(), "a checkpoint must be persisted once");
            for (Long persistedId : persistedIds) {
                Assertions.assertTrue(
                        storedIds.contains(persistedId),
                        "storage returned checkpoint "
                                + persistedId
                                + " that was never written successfully: "
                                + storedIds);
            }
            for (Long rejectedId : rejectedIds) {
                Assertions.assertFalse(
                        persistedIds.contains(rejectedId),
                        "the rejected checkpoint "
                                + rejectedId
                                + " must not be persisted; persisted ids: "
                                + persistedIds);
            }
            long latestPersistedId = Collections.max(persistedIds);
            Assertions.assertTrue(
                    latestPersistedId > Collections.max(rejectedIds),
                    "the restored pipeline must continue the checkpoint id sequence after the "
                            + "rejected ids (rejected="
                            + rejectedIds
                            + ", persisted="
                            + persistedIds
                            + ")");

            List<JobPipelineCheckpointData> selectable =
                    jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT);
            Assertions.assertEquals(
                    1,
                    selectable.size(),
                    "the job must expose exactly one checkpoint for its pipeline: " + selectable);
            long selectableId = selectable.get(0).getCheckpointId();
            Assertions.assertTrue(
                    selectableId >= latestPersistedId,
                    "the client must select the latest successfully persisted checkpoint, but "
                            + "selected "
                            + selectableId
                            + " while storage already held "
                            + latestPersistedId);
            Assertions.assertTrue(
                    storedIds.contains(selectableId),
                    "the selectable checkpoint "
                            + selectableId
                            + " was never written successfully: "
                            + storedIds);
            Assertions.assertFalse(
                    rejectedIds.contains(selectableId),
                    "a checkpoint rejected by storage must never become selectable: "
                            + selectableId);
            Assertions.assertEquals(
                    CheckpointType.CHECKPOINT_TYPE,
                    selectable.get(0).getCheckpointType(),
                    "a periodic checkpoint must remain a periodic checkpoint");
            List<Long> completedIds =
                    checkpointInfos(checkpointMonitor, jobId, CheckpointStatus.COMPLETED).stream()
                            .map(CheckpointInfo::getCheckpointId)
                            .collect(Collectors.toList());
            for (Long rejectedId : rejectedIds) {
                Assertions.assertFalse(
                        completedIds.contains(rejectedId),
                        "a rejected checkpoint must never be reported as COMPLETED; completed="
                                + completedIds
                                + ", rejected="
                                + rejectedIds);
            }

            // ---- Phase 4: the recovered pipeline keeps checkpointing on a stable job.
            long completedBefore =
                    pipelineOverview(checkpointMonitor, jobId).getCounts().getCompleted();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview pipeline =
                                        pipelineOverview(checkpointMonitor, jobId);
                                Assertions.assertTrue(
                                        pipeline.getCounts().getCompleted() > completedBefore,
                                        "the recovered pipeline must keep completing checkpoints: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                                Assertions.assertEquals(
                                        INJECTED_STORE_FAILURES,
                                        pipeline.getCounts().getFailed(),
                                        "no checkpoint may fail after storage recovers: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(jobId),
                                        "the job must stay RUNNING after recovering");
                            });

            // ---- Phase 5: cancellation still cleans the persisted checkpoint data.
            job.cancelJob();
            awaitJobStatus(coordinatorService, jobId, JobStatus.CANCELED);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            getAllCheckpoints(checkpointStorage, jobId).isEmpty(),
                                            "cancelling the job must clean its checkpoint data"));
        } finally {
            if (job != null) {
                try {
                    job.cancelJob();
                } catch (Throwable e) {
                    log.info("Ignoring failure while cancelling job during cleanup", e);
                }
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
            deleteRecursively(namespace);
        }
    }

    private static PipelineCheckpointOverview pipelineOverview(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        PipelineCheckpointOverview pipeline = pipelines(checkpointMonitor, jobId).get(PIPELINE_ID);
        Assertions.assertNotNull(pipeline, "the job's pipeline must be reported");
        return pipeline;
    }

    private static Map<Integer, PipelineCheckpointOverview> pipelines(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(CheckpointOverview::getPipelines)
                .map(HashMap::new)
                .orElseGet(HashMap::new);
    }

    private static List<CheckpointInfo> checkpointInfos(
            CheckpointMonitorService checkpointMonitor, long jobId, CheckpointStatus status) {
        return checkpointMonitor.getHistory(jobId, PIPELINE_ID, HISTORY_LIMIT, status).stream()
                .map(CheckpointHistoryEntry::getCheckpointInfo)
                .collect(Collectors.toList());
    }

    private static List<PipelineState> getAllCheckpoints(
            CheckpointStorage checkpointStorage, long jobId) {
        try {
            return checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail("Failed to list checkpoints of job " + jobId, e);
        }
    }

    private static void awaitJobStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        coordinatorService.getJobStatus(jobId),
                                        "status of job " + jobId));
    }

    private static String describe(Map<Integer, PipelineCheckpointOverview> pipelines) {
        StringBuilder builder = new StringBuilder("{");
        pipelines.forEach(
                (pipelineId, pipeline) ->
                        builder.append("pipeline")
                                .append(pipelineId)
                                .append(": completed=")
                                .append(pipeline.getCounts().getCompleted())
                                .append(", failed=")
                                .append(pipeline.getCounts().getFailed())
                                .append(", restored=")
                                .append(pipeline.getCounts().getRestored())
                                .append(", inProgress=")
                                .append(pipeline.getCounts().getInProgress())
                                .append("; "));
        return builder.append("}").toString();
    }

    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            List<Path> ordered = new ArrayList<>(paths.collect(Collectors.toList()));
            Collections.reverse(ordered);
            for (Path path : ordered) {
                Files.deleteIfExists(path);
            }
        } catch (Exception e) {
            log.warn("Failed to delete test checkpoint directory {}", root, e);
        }
    }
}

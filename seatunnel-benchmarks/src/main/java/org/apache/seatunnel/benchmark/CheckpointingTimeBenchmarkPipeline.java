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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Long-running source-to-sink pipeline whose checkpoint completion time is measured. */
@State(Scope.Thread)
public class CheckpointingTimeBenchmarkPipeline {

    private static final String JOB_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/source-sink-checkpoint.conf.template");
    private static final Duration START_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration CHECKPOINT_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration CANCEL_TIMEOUT = Duration.ofSeconds(30);
    private static final String DEBLOATING_RECORD_SIZE_VALUE = "1b";
    private static final String UNALIGNED_RECORD_SIZE_VALUE = "1kb";
    private static final long SOURCE_RATE_PER_SECOND = 10_000L;
    private static final int PIPELINE_PARALLELISM = 4;

    @Param({"1b", "1kb"})
    public String recordSize;

    private SeaTunnelCheckpointEnvironmentContext environment;
    private ClientJobProxy jobProxy;
    private CheckpointCoordinator checkpointCoordinator;
    private CheckpointMonitorService checkpointMonitorService;
    private int pipelineId;
    private boolean checkpointCompleted;

    /** Submits the streaming pipeline after the checkpoint benchmark cluster has started. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelCheckpointEnvironmentContext environment) throws Exception {
        this.environment = environment;
        try {
            Path jobConfigFile = environment.getClusterHome().resolve("checkpoint-benchmark.conf");
            Path resultPath = environment.getClusterHome().resolve("checkpoint-results");
            Files.write(
                    jobConfigFile,
                    createCheckpointJobConfig(resultPath).getBytes(StandardCharsets.UTF_8));
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("checkpoint-benchmark-" + UUID.randomUUID());
            ClientJobExecutionEnvironment executionEnvironment =
                    environment
                            .getClient()
                            .createExecutionContext(
                                    jobConfigFile.toString(),
                                    jobConfig,
                                    environment.getMasterConfig());
            jobProxy = executionEnvironment.execute();
            waitUntilRunning();
            resolveCheckpointControl();

            // The MASTER node has no worker slots, so RUNNING proves execution is on WORKER.
            Thread.sleep(2_000L);
        } catch (Exception setupFailure) {
            try {
                tearDown();
            } catch (Exception cleanupFailure) {
                setupFailure.addSuppressed(cleanupFailure);
            }
            throw setupFailure;
        }
    }

    /** Stops the streaming pipeline after checking checkpoint and IMap persistence. */
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            if (checkpointCompleted) {
                verifyCheckpointWasPersisted();
            }
            if (jobProxy != null && !jobProxy.getJobStatus().isEndState()) {
                jobProxy.cancelJob();
                waitUntilCanceled();
            }
        } finally {
            environment = null;
            jobProxy = null;
            checkpointCoordinator = null;
            checkpointMonitorService = null;
            checkpointCompleted = false;
        }
    }

    /** Triggers a regular checkpoint and waits until the coordinator records its completion. */
    public void triggerCheckpoint() throws Exception {
        if (checkpointCoordinator == null || checkpointMonitorService == null) {
            throw new IllegalStateException("Checkpoint benchmark pipeline has not been started");
        }
        long previousCheckpointId = latestCompletedCheckpointId();
        PassiveCompletableFuture<CompletedCheckpoint> checkpoint =
                CheckpointBenchmarkTrigger.trigger(checkpointCoordinator);
        checkpoint.get(CHECKPOINT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        long deadline = System.nanoTime() + CHECKPOINT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (latestCompletedCheckpointId() > previousCheckpointId
                    && !CheckpointBenchmarkTrigger.hasPendingCheckpoint(checkpointCoordinator)) {
                checkpointCompleted = true;
                return;
            }
            Thread.sleep(1L);
        }
        throw new IllegalStateException("Timed out waiting for benchmark checkpoint to complete");
    }

    String createCheckpointJobConfig(Path resultPath) {
        return BenchmarkTemplates.render(
                JOB_TEMPLATE,
                "payload_size",
                selectedRecordSize().getBytes(),
                "source_rate_per_second",
                SOURCE_RATE_PER_SECOND,
                "pipeline_parallelism",
                PIPELINE_PARALLELISM,
                "result_path",
                resultPath.toAbsolutePath());
    }

    private void waitUntilRunning() throws InterruptedException {
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            JobStatus status = jobProxy.getJobStatus();
            if (status == JobStatus.RUNNING) {
                return;
            }
            if (status.isEndState()) {
                throw new IllegalStateException(
                        "Checkpoint benchmark job ended during setup with " + status);
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Timed out waiting for checkpoint benchmark job to run");
    }

    private void waitUntilCanceled() throws InterruptedException {
        long deadline = System.nanoTime() + CANCEL_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (jobProxy.getJobStatus().isEndState()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new IllegalStateException("Timed out canceling checkpoint benchmark job");
    }

    private void resolveCheckpointControl() {
        SeaTunnelServer server =
                environment
                        .getMasterInstance()
                        .node
                        .getNodeEngine()
                        .getService(SeaTunnelServer.SERVICE_NAME);
        if (!server.isMasterNode()) {
            throw new IllegalStateException("Checkpoint benchmark master node is not active");
        }
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobProxy.getJobId());
        if (jobMaster == null) {
            throw new IllegalStateException("Checkpoint benchmark JobMaster is unavailable");
        }
        List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
        if (pipelines.size() != 1) {
            throw new IllegalStateException(
                    "Checkpoint benchmark requires one pipeline but found " + pipelines.size());
        }
        pipelineId = pipelines.get(0).getPipelineId();
        checkpointCoordinator =
                jobMaster.getCheckpointManager().getCheckpointCoordinator(pipelineId);
        checkpointMonitorService = server.getCheckpointMonitorService();
    }

    private long latestCompletedCheckpointId() {
        return checkpointMonitorService
                .getHistory(jobProxy.getJobId(), pipelineId, 1, CheckpointStatus.COMPLETED).stream()
                .map(CheckpointHistoryEntry::getCheckpointInfo)
                .mapToLong(CheckpointInfo::getCheckpointId)
                .findFirst()
                .orElse(-1L);
    }

    private void verifyCheckpointWasPersisted() throws Exception {
        verifyPersistenceDirectory(
                environment.getCheckpointStorageDirectory(), ".ser", "checkpoint");
        verifyPersistenceDirectory(environment.getMapStoreDirectory(), null, "MapStore");
    }

    private static void verifyPersistenceDirectory(
            Path directory, String fileSuffix, String storageName) throws Exception {
        if (!Files.isDirectory(directory)) {
            throw new IllegalStateException(
                    storageName + " storage directory was not created: " + directory);
        }
        try (Stream<Path> files = Files.walk(directory)) {
            if (files.noneMatch(
                    path ->
                            Files.isRegularFile(path)
                                    && (fileSuffix == null
                                            || path.getFileName()
                                                    .toString()
                                                    .endsWith(fileSuffix)))) {
                throw new IllegalStateException(
                        "No persisted " + storageName + " state was found under " + directory);
            }
        }
    }

    private SeaTunnelCheckpointEnvironmentContext.MemorySize selectedRecordSize() {
        if (DEBLOATING_RECORD_SIZE_VALUE.equals(recordSize)) {
            return SeaTunnelCheckpointEnvironmentContext.DEBLOATING_RECORD_SIZE;
        }
        if (UNALIGNED_RECORD_SIZE_VALUE.equals(recordSize)) {
            return SeaTunnelCheckpointEnvironmentContext.UNALIGNED_RECORD_SIZE;
        }
        throw new IllegalArgumentException("Unsupported checkpoint record size: " + recordSize);
    }
}

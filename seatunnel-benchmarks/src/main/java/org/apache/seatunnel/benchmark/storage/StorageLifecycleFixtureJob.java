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

package org.apache.seatunnel.benchmark.storage;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.benchmark.BenchmarkTemplates;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.map.IMap;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Runs one real Zeta job to produce checkpoint and IMap fixtures outside JMH timing. */
public final class StorageLifecycleFixtureJob implements AutoCloseable {

    private static final String SIMPLE_JOB_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/storage-lifecycle-fixture-job.conf.template");
    private static final Duration START_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration CHECKPOINT_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration STOP_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration DEFAULT_CHECKPOINT_INTERVAL = Duration.ofDays(1);

    private final SeaTunnelStorageEnvironmentContext environment;
    private final Duration checkpointInterval;
    private ClientJobProxy jobProxy;
    private CheckpointCoordinator checkpointCoordinator;
    private int pipelineId;

    public StorageLifecycleFixtureJob(SeaTunnelStorageEnvironmentContext environment) {
        this(environment, DEFAULT_CHECKPOINT_INTERVAL);
    }

    public StorageLifecycleFixtureJob(
            SeaTunnelStorageEnvironmentContext environment, Duration checkpointInterval) {
        this.environment = environment;
        if (checkpointInterval.isZero() || checkpointInterval.isNegative()) {
            throw new IllegalArgumentException("Checkpoint interval must be positive");
        }
        this.checkpointInterval = checkpointInterval;
    }

    /** Starts the real streaming fixture and resolves its production checkpoint coordinator. */
    public void start() throws Exception {
        try {
            Path jobFile = environment.storageHome().resolve("storage-lifecycle-fixture-job.conf");
            Path resultDirectory = environment.storageHome().resolve("storage-fixture-results");
            Files.write(
                    jobFile,
                    BenchmarkTemplates.render(
                                    SIMPLE_JOB_TEMPLATE,
                                    "result_path",
                                    resultDirectory.toAbsolutePath(),
                                    "run_id",
                                    UUID.randomUUID(),
                                    "checkpoint_interval",
                                    checkpointInterval.toMillis())
                            .getBytes(StandardCharsets.UTF_8));

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("storage-fixture-" + UUID.randomUUID());
            ClientJobExecutionEnvironment executionEnvironment =
                    environment
                            .storageClient()
                            .createExecutionContext(
                                    jobFile.toString(), jobConfig, environment.storageConfig());
            jobProxy = executionEnvironment.execute();
            waitUntilRunning();
            resolveCheckpointCoordinator();
            Thread.sleep(2_000L);
        } catch (Exception setupFailure) {
            try {
                close();
            } catch (Exception cleanupFailure) {
                setupFailure.addSuppressed(cleanupFailure);
            }
            throw setupFailure;
        }
    }

    /** Waits for the production scheduler to finish one regular checkpoint. */
    public long awaitCompletedCheckpoint() throws Exception {
        long deadline = System.nanoTime() + CHECKPOINT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            List<CheckpointHistoryEntry> completed =
                    environment
                            .getServer()
                            .getCheckpointMonitorService()
                            .getHistory(getJobId(), pipelineId, 1, CheckpointStatus.COMPLETED);
            if (!completed.isEmpty() && !hasPendingCheckpoint()) {
                return completed.get(0).getCheckpointInfo().getCheckpointId();
            }
            Thread.sleep(1L);
        }
        throw new IllegalStateException("Timed out waiting for the storage fixture checkpoint");
    }

    public long getJobId() {
        return jobProxy.getJobId();
    }

    /** Returns the real running-job submission payload containing the serialized logical DAG. */
    public JobInfo runningJobInfo() {
        IMap<Long, JobInfo> map =
                environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JobInfo value = map.get(getJobId());
        if (value == null) {
            throw new IllegalStateException("The running JobInfo fixture is unavailable");
        }
        return value;
    }

    /** Cancels the job normally so Zeta produces finished state, metrics, and DAG fixtures. */
    public void finish() throws Exception {
        if (!jobProxy.getJobStatus().isEndState()) {
            jobProxy.cancelJob();
        }
        waitUntilStopped();
        waitUntilFinishedFixturesExist();
        waitUntilRuntimeStateWasCleaned();
    }

    public JobMetrics finishedMetrics() {
        return requiredFinishedValue(Constant.IMAP_FINISHED_JOB_METRICS, JobMetrics.class);
    }

    public JobHistoryService.JobState finishedState() {
        return requiredFinishedValue(
                Constant.IMAP_FINISHED_JOB_STATE, JobHistoryService.JobState.class);
    }

    @Override
    public void close() throws Exception {
        try {
            if (jobProxy != null) {
                if (!jobProxy.getJobStatus().isEndState()) {
                    jobProxy.cancelJob();
                }
                waitUntilStopped();
                waitUntilRuntimeStateWasCleaned();
            }
        } finally {
            jobProxy = null;
            checkpointCoordinator = null;
        }
    }

    private void resolveCheckpointCoordinator() {
        SeaTunnelServer server = environment.getServer();
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobProxy.getJobId());
        if (jobMaster == null) {
            throw new IllegalStateException("Storage fixture JobMaster is unavailable");
        }
        List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
        if (pipelines.size() != 1) {
            throw new IllegalStateException(
                    "Storage fixture requires one pipeline but found " + pipelines.size());
        }
        pipelineId = pipelines.get(0).getPipelineId();
        checkpointCoordinator =
                jobMaster.getCheckpointManager().getCheckpointCoordinator(pipelineId);
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
                        "Storage fixture ended during setup with " + status);
            }
            Thread.sleep(50L);
        }
        throw new IllegalStateException("Timed out waiting for the storage fixture to run");
    }

    private void waitUntilStopped() throws InterruptedException {
        long deadline = System.nanoTime() + STOP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (jobProxy.getJobStatus().isEndState()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new IllegalStateException("Timed out stopping the storage fixture job");
    }

    private void waitUntilFinishedFixturesExist() throws InterruptedException {
        long deadline = System.nanoTime() + STOP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (finishedMap(Constant.IMAP_FINISHED_JOB_STATE).containsKey(getJobId())
                    && finishedMap(Constant.IMAP_FINISHED_JOB_METRICS).containsKey(getJobId())) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new IllegalStateException("Timed out waiting for finished storage fixtures");
    }

    private void waitUntilRuntimeStateWasCleaned() throws InterruptedException {
        long deadline = System.nanoTime() + STOP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (runtimeStateWasCleaned()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new IllegalStateException("Timed out waiting for storage fixture state cleanup");
    }

    private boolean runtimeStateWasCleaned() {
        long jobId = getJobId();
        // Production removes the pending record only after deleting the job's state and timestamp
        // entries. Checking both maps also avoids treating the short pre-scheduling window as a
        // completed cleanup.
        return !finishedMap(Constant.IMAP_RUNNING_JOB_INFO).containsKey(jobId)
                && !finishedMap(Constant.IMAP_PENDING_JOB_CLEANUP).containsKey(jobId);
    }

    private <T> T requiredFinishedValue(String mapName, Class<T> valueType) {
        Object value = finishedMap(mapName).get(getJobId());
        if (!valueType.isInstance(value)) {
            throw new IllegalStateException(
                    "Missing " + valueType.getSimpleName() + " fixture in " + mapName);
        }
        return valueType.cast(value);
    }

    private IMap<Long, Object> finishedMap(String mapName) {
        return environment.getServer().getNodeEngine().getHazelcastInstance().getMap(mapName);
    }

    private boolean hasPendingCheckpoint() {
        AtomicInteger pendingCounter =
                (AtomicInteger)
                        ReflectionUtils.getField(checkpointCoordinator, "pendingCounter")
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Checkpoint pending counter is unavailable"));
        return pendingCounter.get() > 0;
    }
}

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
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.InProgressCheckpoint;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.task.TransformSeaTunnelTask;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * End-to-end regression coverage for losing a worker while a stop-with-savepoint barrier is still
 * unacknowledged.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A stop-with-savepoint request is a
 * job-level stop command whose only legal outcomes are "savepoint completed" or "savepoint failed
 * and the job still stopped". A worker that disappears mid-barrier creates two competing state
 * machines at once: {@code SubPlan} marks the failed task group FAILED and restores the pipeline
 * from the job's latest checkpoint, which calls {@code CheckpointCoordinator#restoreCoordinator()}
 * and aborts the pending savepoint, while {@code JobMaster#savePoint()} observes that abort and
 * calls {@code PhysicalPlan#savepointFailed()}. That fallback only stops the job when the job is
 * still {@link JobStatus#DOING_SAVEPOINT}; if the restore has already moved the plan back to {@link
 * JobStatus#RUNNING}, the stop intent is silently dropped and the job keeps running with a
 * savepoint that never completed. This cannot be observed from task errors or the savepoint
 * directory alone: the client gets a failure either way, so only the job's final status and a
 * follow-up savepoint attempt reveal whether the stop command survived.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code CancelDuringSavepointIT} cancels a
 * job whose savepoint barrier is parked; there the cancel request is a second live command, not a
 * task failure, and the job ends CANCELED by construction. {@code CheckpointCoordinatorFailoverIT}
 * covers periodic checkpoints and master failover, not a worker loss inside a savepoint. {@code
 * SavePointBusySourceTest#testSavepointFailureReportsErrorAndStopsJob} covers a savepoint that
 * times out while every worker stays healthy, so the restore path never competes with the savepoint
 * failure. {@code SplitClusterWorkerRestartAfterFinalCheckpointIT} covers a worker loss after a
 * final checkpoint, not while a savepoint barrier is pending. No existing test kills the worker
 * that owns an unacknowledged savepoint barrier while another worker is available to restore the
 * pipeline.
 *
 * <p><b>How this test pins the window.</b> The job uses a huge checkpoint interval, so the first
 * checkpoint after the restore is the savepoint the test requests. {@link
 * org.apache.seatunnel.engine.e2e.savepointworker.SavepointWorkerHoldSink} publishes the checkpoint
 * id in a marker file and parks inside {@code snapshotState(long)} until the test creates the
 * release marker. The test verifies through the master's checkpoint overview that the parked
 * checkpoint is an in-progress {@code SAVEPOINT_TYPE} checkpoint, locates the physical sink vertex
 * and shuts down exactly the worker that executes it, then releases the marker. A second worker
 * stays alive, so the pipeline really restores instead of failing for lack of resources.
 *
 * <p>The test then requires: the client's savepoint request fails rather than reporting success;
 * the job reaches a terminal state (CANCELED or FAILED) instead of staying RUNNING or stuck in
 * DOING_SAVEPOINT; the aborted savepoint is never stored as a completed recovery point; and a
 * follow-up job can still be scheduled on the surviving master/worker pair.
 */
@Slf4j
public class WorkerFailureDuringSavepointIT {

    /** Name of the test resource template rendered into the actual job config file. */
    private static final String TEST_TEMPLATE_FILE_NAME =
            "stream_fake_to_savepoint_worker_hold_template.conf";

    /** Placeholder key in the template that is substituted with the marker directory. */
    private static final String DYNAMIC_HOLD_DIR = "dynamic_hold_dir";

    /** Placeholder key in the template that is substituted with the per-run marker stem. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Placeholder key in the template that is substituted with the held snapshot number. */
    private static final String DYNAMIC_BLOCK_ON_SNAPSHOT = "dynamic_block_on_snapshot";

    /**
     * The savepoint is the only checkpoint of its execution round, so its snapshot is the first.
     */
    private static final int BLOCK_ON_SAVEPOINT_SNAPSHOT = 1;

    /** Upper bound to wait for the one-master/two-worker cluster to fully form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the submitted or restored job to reach RUNNING. */
    private static final long JOB_RUNNING_TIMEOUT_SECONDS = 90L;

    /** Upper bound to wait for the savepoint request to move the job into DOING_SAVEPOINT. */
    private static final long SAVEPOINT_START_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the held snapshot to publish its marker. */
    private static final long HOLD_ENTRY_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the master to report the parked checkpoint as a savepoint. */
    private static final long PENDING_SAVEPOINT_TIMEOUT_SECONDS = 60L;

    /** Upper bound for the in-flight savepoint request to resolve after the worker is killed. */
    private static final long SAVEPOINT_RESULT_TIMEOUT_SECONDS = 180L;

    /** Upper bound for the job to leave DOING_SAVEPOINT and reach a terminal state. */
    private static final long JOB_TERMINAL_TIMEOUT_SECONDS = 300L;

    /** Upper bound for the follow-up job to finish on the surviving cluster. */
    private static final long FOLLOW_UP_JOB_TIMEOUT_SECONDS = 180L;

    @Test
    public void testWorkerFailureDuringInFlightSavepointStopsTheJob() throws Exception {
        String testCaseName = "testWorkerFailureDuringInFlightSavepointStopsTheJob";
        String testClusterName = "WorkerFailureDuringSavepointIT_" + testCaseName;
        Path holdDirectory =
                Paths.get(
                        System.getProperty("user.dir"),
                        "target",
                        "savepoint-worker-failure",
                        Long.toString(System.nanoTime()));
        String holdKey = testCaseName;
        Files.createDirectories(holdDirectory);
        Path heldMarker = holdDirectory.resolve(holdKey + ".held");
        Path releaseMarker = holdDirectory.resolve(holdKey + ".release");
        // The first savepoint must not be held: only the savepoint requested after the restore is
        // the window under test.
        Files.write(releaseMarker, new byte[0]);

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        ExecutorService savepointExecutor = Executors.newSingleThreadExecutor();
        try {
            masterNode =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode1 =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode2 =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            awaitClusterSize(masterNode, 3);
            awaitWorkersRegistered(masterNode, 2);
            HazelcastInstanceImpl activeMaster = masterNode;

            Common.setDeployMode(DeployMode.CLIENT);
            String confPath =
                    createTestResources(
                            testCaseName,
                            holdDirectory.toString(),
                            holdKey,
                            BLOCK_ON_SAVEPOINT_SNAPSHOT);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            ClientJobProxy clientJobProxy =
                    engineClient
                            .createExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName))
                            .execute();
            long jobId = clientJobProxy.getJobId();
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            awaitCheckpointCoordinatorReady(masterNode, jobId);

            JobClient jobClient = engineClient.createJobClient();

            // Establish a real recovery point first. Without it the restore after the worker loss
            // would have no checkpoint to resume from and could not reach RUNNING, which would
            // mask the very state-machine race this test targets.
            engineClient.savePointJob(jobId);
            awaitJobStatus(clientJobProxy, JobStatus.SAVEPOINT_DONE);
            long firstSavepointId =
                    checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT).stream()
                            .mapToLong(JobPipelineCheckpointData::getCheckpointId)
                            .max()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "The first savepoint was not persisted"));
            log.warn("Job {} completed its recovery savepoint {}", jobId, firstSavepointId);

            ClientJobProxy restoredProxy =
                    engineClient
                            .restoreExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName), jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId, restoredProxy.getJobId(), "A savepoint restore must keep the job id");
            awaitJobStatus(restoredProxy, JobStatus.RUNNING);
            awaitCheckpointCoordinatorReady(masterNode, jobId);

            JobMaster jobMaster = awaitJobMaster(masterNode, jobId);
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            Assertions.assertEquals(1, pipelines.size(), "The test job must have one pipeline");
            SubPlan pipeline = pipelines.get(0);
            int pipelineId = pipeline.getPipelineId();

            // Arm the hold and request the savepoint whose barrier will be parked.
            Files.delete(releaseMarker);
            AtomicReference<Throwable> savepointFailure = new AtomicReference<>();
            SeaTunnelClient finalEngineClient = engineClient;
            CompletableFuture<Void> savepointRequest =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    finalEngineClient.savePointJob(jobId);
                                } catch (Throwable t) {
                                    savepointFailure.set(t);
                                    throw new RuntimeException(t);
                                }
                            },
                            savepointExecutor);

            Awaitility.await()
                    .atMost(SAVEPOINT_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.DOING_SAVEPOINT,
                                            restoredProxy.getJobStatus(),
                                            "The savepoint request did not move the job into"
                                                    + " DOING_SAVEPOINT"));

            long heldCheckpointId =
                    awaitHeldCheckpoint(heldMarker, HOLD_ENTRY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.assertTrue(
                    heldCheckpointId > firstSavepointId,
                    "The held checkpoint must be newer than the recovery savepoint, but was "
                            + heldCheckpointId
                            + " (recovery savepoint "
                            + firstSavepointId
                            + ")");
            Assertions.assertFalse(
                    savepointRequest.isDone(),
                    "The savepoint must still be in flight while its barrier is unacknowledged");

            Awaitility.await()
                    .atMost(PENDING_SAVEPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview overview =
                                        pipelineOverview(activeMaster, jobId, pipelineId);
                                Assertions.assertNotNull(
                                        overview,
                                        "The checkpoint overview is not available while the"
                                                + " savepoint is parked");
                                InProgressCheckpoint pending =
                                        overview.getInProgress().stream()
                                                .filter(
                                                        checkpoint ->
                                                                checkpoint.getCheckpointId()
                                                                        == heldCheckpointId)
                                                .findFirst()
                                                .orElse(null);
                                Assertions.assertNotNull(
                                        pending,
                                        "The held checkpoint is not reported as in progress");
                                Assertions.assertEquals(
                                        CheckpointType.SAVEPOINT_TYPE,
                                        pending.getCheckpointType(),
                                        "The held checkpoint must be the requested savepoint");
                            });

            PhysicalVertex sinkVertex = awaitRunningSinkVertex(pipeline);
            Address sinkAddress = sinkVertex.getCurrentExecutionAddress();
            Assertions.assertNotNull(sinkAddress, "The sink task has no execution address");
            HazelcastInstanceImpl workerToKill = findWorker(sinkAddress, workerNode1, workerNode2);
            Assertions.assertNotNull(
                    workerToKill,
                    "The sink task is not running on either scripted worker: " + sinkAddress);

            log.warn(
                    "Killing worker {} that hosts the parked savepoint sink task {} of job {}",
                    sinkAddress,
                    sinkVertex.getTaskFullName(),
                    jobId);
            workerToKill.shutdown();
            if (workerToKill == workerNode1) {
                workerNode1 = null;
            } else {
                workerNode2 = null;
            }
            // The parked writer died with its worker. The marker lets the writer of the restored
            // pipeline pass through immediately, so this test does not hold the restore itself.
            Files.write(releaseMarker, new byte[0]);

            assertSavepointRequestFailed(savepointRequest, savepointFailure);

            // The stop request must survive the competing pipeline restore: the job must leave
            // DOING_SAVEPOINT and end, never continue as RUNNING with a savepoint that never
            // completed.
            Awaitility.await()
                    .atMost(JOB_TERMINAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = restoredProxy.getJobStatus();
                                Assertions.assertTrue(
                                        status == JobStatus.CANCELED || status == JobStatus.FAILED,
                                        "A failed stop-with-savepoint must stop the job, but the"
                                                + " status is "
                                                + status);
                            });
            JobStatus terminalStatus = restoredProxy.getJobStatus();
            for (int i = 0; i < 10; i++) {
                Assertions.assertEquals(
                        terminalStatus,
                        restoredProxy.getJobStatus(),
                        "The terminal state must be stable after the aborted savepoint");
                Thread.sleep(200L);
            }

            // The aborted savepoint must not become a recovery point.
            List<JobPipelineCheckpointData> savepoints =
                    checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT);
            Assertions.assertTrue(
                    savepoints.stream()
                            .noneMatch(data -> data.getCheckpointId() == heldCheckpointId),
                    "The aborted savepoint must not be stored as a completed savepoint");
            Assertions.assertEquals(
                    firstSavepointId,
                    savepoints.stream()
                            .mapToLong(JobPipelineCheckpointData::getCheckpointId)
                            .max()
                            .orElse(-1L),
                    "The failed stop-with-savepoint must not create another savepoint");

            // The surviving master and worker must still schedule jobs; otherwise the failure
            // above would say nothing about a clean terminal transition.
            submitFollowUpJob(engineClient, testClusterName);

            log.warn(
                    "Job {} ended {} after worker {} was lost during savepoint checkpoint {}",
                    jobId,
                    terminalStatus,
                    sinkAddress,
                    heldCheckpointId);
        } finally {
            Files.deleteIfExists(releaseMarker);
            Files.deleteIfExists(heldMarker);
            savepointExecutor.shutdownNow();
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode1 != null) {
                workerNode1.shutdown();
            }
            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    private static void assertSavepointRequestFailed(
            CompletableFuture<Void> savepointRequest, AtomicReference<Throwable> savepointFailure)
            throws Exception {
        try {
            savepointRequest.get(SAVEPOINT_RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.fail(
                    "The in-flight savepoint request completed normally although its sink task"
                            + " was lost before the barrier was acknowledged");
        } catch (ExecutionException expected) {
            Assertions.assertNotNull(
                    expected.getCause(), "The failed savepoint request must carry its cause");
            Assertions.assertNotNull(
                    savepointFailure.get(),
                    "The failed savepoint request must record the engine failure");
            log.warn(
                    "In-flight savepoint failed as expected after the worker loss: {}",
                    savepointFailure.get().toString());
        } catch (TimeoutException e) {
            Assertions.fail(
                    "The in-flight savepoint request did not resolve within "
                            + SAVEPOINT_RESULT_TIMEOUT_SECONDS
                            + " s after the worker was killed");
        }
    }

    private static void submitFollowUpJob(SeaTunnelClient engineClient, String testClusterName) {
        JobConfig followUpJobConfig = new JobConfig();
        followUpJobConfig.setName("worker-failure-savepoint-follow-up");
        String followUpConfPath = TestUtils.getResource("fakesource_to_console.conf");
        ClientJobProxy followUpProxy;
        try {
            followUpProxy =
                    engineClient
                            .createExecutionContext(
                                    followUpConfPath,
                                    followUpJobConfig,
                                    getSeaTunnelConfig(testClusterName))
                            .execute();
        } catch (Exception e) {
            throw new AssertionError("The surviving cluster rejected a follow-up job", e);
        }
        Awaitility.await()
                .atMost(FOLLOW_UP_JOB_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        followUpProxy.getJobStatus(),
                                        "The surviving cluster did not finish a follow-up job"));
    }

    private static long awaitHeldCheckpoint(Path heldMarker, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadlineNanos) {
            if (Files.isRegularFile(heldMarker)) {
                try {
                    String content =
                            new String(Files.readAllBytes(heldMarker), StandardCharsets.UTF_8)
                                    .trim();
                    if (!content.isEmpty()) {
                        return Long.parseLong(content);
                    }
                } catch (IOException | NumberFormatException ignored) {
                    // The writer publishes the marker atomically, so a failed read is transient.
                }
            }
            Thread.sleep(50L);
        }
        throw new AssertionError(
                "The sink did not park in the savepoint snapshot within "
                        + unit.toMillis(timeout)
                        + " ms");
    }

    /**
     * Returns the running task-group vertex that owns the sink writer of the single sink chain.
     *
     * <p>Zeta chains the source, transform and sink flow lifecycles of one parallelism unit into
     * the same task group. The sink writer therefore does not appear as a separate {@code
     * Sink[...]} physical vertex: it is driven by the group's {@link TransformSeaTunnelTask}.
     * Locating the group through that task is what lets the test kill exactly the worker that owns
     * the parked snapshot barrier.
     */
    private static PhysicalVertex awaitRunningSinkVertex(SubPlan pipeline) {
        AtomicReference<PhysicalVertex> result = new AtomicReference<>();
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<PhysicalVertex> vertices =
                                    Stream.concat(
                                                    pipeline.getCoordinatorVertexList().stream(),
                                                    pipeline.getPhysicalVertexList().stream())
                                            .collect(Collectors.toList());
                            PhysicalVertex sinkGroup =
                                    vertices.stream()
                                            .filter(
                                                    vertex ->
                                                            vertex.getTaskGroup().getTasks()
                                                                    .stream()
                                                                    .anyMatch(
                                                                            task ->
                                                                                    task
                                                                                            instanceof
                                                                                            TransformSeaTunnelTask))
                                            .findFirst()
                                            .orElse(null);
                            Assertions.assertNotNull(
                                    sinkGroup,
                                    "The task group driving the sink writer is not available");
                            Assertions.assertEquals(
                                    ExecutionState.RUNNING,
                                    sinkGroup.getExecutionState(),
                                    "The task group driving the sink writer is not running");
                            Assertions.assertNotNull(
                                    sinkGroup.getCurrentExecutionAddress(),
                                    "The task group driving the sink writer has no execution"
                                            + " address yet");
                            result.set(sinkGroup);
                        });
        return result.get();
    }

    private static HazelcastInstanceImpl findWorker(
            Address address, HazelcastInstanceImpl workerNode1, HazelcastInstanceImpl workerNode2) {
        for (HazelcastInstanceImpl worker :
                new HazelcastInstanceImpl[] {workerNode1, workerNode2}) {
            if (worker == null) {
                continue;
            }
            if (worker.getCluster().getLocalMember().getAddress().equals(address)) {
                return worker;
            }
        }
        return null;
    }

    private static List<JobPipelineCheckpointData> checkpointData(
            JobClient jobClient, long jobId, RestoreMode restoreMode) {
        return jobClient.getCheckpointData(jobId, restoreMode);
    }

    /**
     * Waits until every pipeline's checkpoint coordinator accepted all task READY_START reports.
     *
     * <p>The job-level status turns RUNNING before the coordinator's own {@code isAllTaskReady}
     * flag does, and a savepoint requested inside that gap is rejected with {@code
     * TASK_NOT_ALL_READY_WHEN_SAVEPOINT}. The test must start from a coordinator that can really
     * accept the savepoint, otherwise the precondition failure would be mistaken for the behavior
     * under test.
     */
    private static void awaitCheckpointCoordinatorReady(
            HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = getJobMaster(masterNode, jobId);
                            Assertions.assertNotNull(
                                    jobMaster, "The job master is not available yet");
                            for (SubPlan pipeline : jobMaster.getPhysicalPlan().getPipelineList()) {
                                CheckpointCoordinator coordinator =
                                        jobMaster
                                                .getCheckpointManager()
                                                .getCheckpointCoordinator(pipeline.getPipelineId());
                                Assertions.assertNotNull(
                                        coordinator,
                                        "The checkpoint coordinator is not available yet");
                                AtomicBoolean ready =
                                        (AtomicBoolean)
                                                ReflectionUtils.getField(
                                                                coordinator, "isAllTaskReady")
                                                        .orElseThrow(
                                                                () ->
                                                                        new IllegalStateException(
                                                                                "Missing isAllTaskReady field"));
                                Assertions.assertTrue(
                                        ready.get(),
                                        "Not every task of pipeline "
                                                + pipeline.getPipelineId()
                                                + " reported READY_START yet");
                            }
                        });
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        jobProxy.getJobStatus(),
                                        "Waiting for job status " + expected));
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expected) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected, node.getCluster().getMembers().size()));
    }

    private static void awaitWorkersRegistered(
            HazelcastInstanceImpl masterNode, int expectedWorkers) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SeaTunnelServer server = seaTunnelServer(masterNode);
                            Assertions.assertNotNull(
                                    server.getCoordinatorService(),
                                    "The coordinator service is not active yet");
                            Assertions.assertEquals(
                                    expectedWorkers,
                                    server.getCoordinatorService()
                                            .getResourceManager()
                                            .workerCount(java.util.Collections.emptyMap()),
                                    "Waiting for every worker to register its slots");
                        });
    }

    private static JobMaster awaitJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> getJobMaster(masterNode, jobId) != null);
        return getJobMaster(masterNode, jobId);
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server = seaTunnelServer(masterNode);
        return server.getCoordinatorService() == null
                ? null
                : server.getCoordinatorService().getJobMaster(jobId);
    }

    private static SeaTunnelServer seaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    private static PipelineCheckpointOverview pipelineOverview(
            HazelcastInstanceImpl masterNode, long jobId, int pipelineId) {
        SeaTunnelServer server = seaTunnelServer(masterNode);
        CheckpointOverview overview =
                server.getEngineContext()
                        .getStateStores()
                        .auxiliary()
                        .checkpointOverviewStateStore()
                        .get(jobId);
        return overview == null ? null : overview.getPipelines().get(pipelineId);
    }

    private static String createTestResources(
            String testCaseName, String holdDirectory, String holdKey, int blockOnSnapshot)
            throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_DIR, holdDirectory);
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        valueMap.put(DYNAMIC_BLOCK_ON_SNAPSHOT, String.valueOf(blockOnSnapshot));

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                TEST_TEMPLATE_FILE_NAME, valueMap, targetConfigFilePath);
        return targetConfigFilePath;
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }
}

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
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Regression guard for the isolation contract between co-located jobs in one Zeta worker: stopping
 * one job must only release that job's own fixed slots and must not disturb the checkpoint chain or
 * the slot ownership of a sibling job that keeps running on the same worker.
 *
 * <p>Scenario: a single master-and-worker node advertises exactly {@link #SLOT_NUM} fixed slots
 * ({@code dynamic-slot = false}, {@code job-schedule-strategy = WAIT}). Two long-lived streaming
 * jobs each occupy {@link #SLOTS_PER_JOB} of them. The sibling job uses a deliberately slow sink
 * checkpoint (the test {@code InMemory} sink's {@code checkpoint_sleep} option makes {@code
 * prepareCommit()} sleep 5 s), so its checkpoint is in flight for most of the 1 s checkpoint
 * interval and the cancellation below lands inside a checkpoint rather than between two
 * checkpoints. The test then cancels the first job and, after the sibling job keeps checkpointing
 * across that cancellation, submits a replacement job with the cancelled job's slot footprint.
 *
 * <p>Contract asserted here:
 *
 * <ol>
 *   <li>Releasing a cancelled job must be job-scoped: every fixed slice it owned becomes free,
 *       every slice the sibling still owns keeps its identity, and the sibling keeps exactly its
 *       own slices (a release keyed by worker, tag or slot index rather than by owner would steal
 *       them).
 *   <li>The sibling's checkpoint chain must survive the co-located cancellation: it keeps
 *       completing new checkpoints, its checkpoint ids stay unique and increasing, and no
 *       checkpoint of it is reported failed and no pipeline restore of it is recorded.
 *   <li>The freed capacity must be reusable immediately by a replacement job while the sibling
 *       keeps running, and the two jobs must not share slot ownership; the replacement job starts
 *       its own checkpoint sequence at {@link #FIRST_CHECKPOINT_ID} instead of inheriting the
 *       cancelled job's counters.
 * </ol>
 *
 * <p>Existing coverage does not exercise this window. {@code SeaTunnelSlotIT} submits jobs one at a
 * time and only checks that a later job finds free capacity afterwards; {@code
 * SavepointBusySourceBarrierIT}, {@code CheckpointCoordinatorFailoverIT} and {@code
 * SplitClusterPendingJobLifecycleFailoverIT} all let a single job own the worker (or fail it over),
 * so no existing engine E2E asserts that a cancellation is scoped to its own job's slots and
 * checkpoint bookkeeping while a second job keeps running next to it.
 */
@Slf4j
public class SiblingJobIsolationOnCancelIT {

    private static final String FAST_CHECKPOINT_CONF =
            "sibling-job-isolation/stream_fake_to_inmemory_fast_checkpoint.conf";

    private static final String SLOW_CHECKPOINT_CONF =
            "sibling-job-isolation/stream_fake_to_inmemory_slow_checkpoint.conf";

    /** Fixed size of the single worker's slot pool: two jobs of {@link #SLOTS_PER_JOB} slices. */
    private static final int SLOT_NUM = 6;

    /**
     * Slices one job in this test occupies: the {@code FakeSource} reader task group at parallelism
     * 1, the split-enumerator task group of that source, and the sink task group, because the test
     * {@code InMemory} sink has an aggregated committer and therefore runs as its own task group
     * (see {@code PhysicalPlanGenerator#splitSinkFromFlow}).
     */
    private static final int SLOTS_PER_JOB = 3;

    /** First checkpoint id issued for a fresh job. */
    private static final long FIRST_CHECKPOINT_ID = 1L;

    /** Sleep injected by the {@code InMemory} sink's {@code checkpoint_sleep} option. */
    private static final long SIBLING_CHECKPOINT_SLEEP_MILLIS = 5000L;

    private static final int HISTORY_LIMIT = 500;

    private HazelcastInstanceImpl node;
    private Address workerAddress;
    private ResourceManager resourceManager;
    private CheckpointMonitorService checkpointMonitor;

    @Test
    public void testCancellingOneJobKeepsSiblingJobCheckpointsAndSlots() throws Exception {
        String testCaseName = "testCancellingOneJobKeepsSiblingJobCheckpointsAndSlots";
        String clusterName = "SiblingJobIsolationOnCancelIT_" + testCaseName;

        SeaTunnelClient engineClient = null;
        ClientJobProxy firstJob = null;
        ClientJobProxy siblingJob = null;
        ClientJobProxy replacementJob = null;

        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Fixed slices: this contract is about accounting for owned slices, which the default
            // dynamic-slot mode does not track at all.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
            // WAIT keeps a job that cannot fit yet queued instead of failing it, so the replacement
            // job below exercises reuse of the released slices rather than an admission race.
            seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            NodeEngineImpl nodeEngine = node.node.nodeEngine;
            workerAddress = node.node.address;
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = server.getCoordinatorService();
            resourceManager = coordinatorService.getResourceManager();
            checkpointMonitor = server.getCheckpointMonitorService();

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            workerProfile().getUnassignedSlots().length == SLOT_NUM,
                                            "worker should advertise all fixed slices"));

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            // ---- Phase 1: the first job owns its slices and checkpoints on its own.
            firstJob =
                    submit(
                            engineClient,
                            FAST_CHECKPOINT_CONF,
                            "sibling-isolation-first",
                            seaTunnelConfig);
            long firstJobId = firstJob.getJobId();

            awaitStatus(coordinatorService, firstJobId, JobStatus.RUNNING);
            awaitJobOwnsSlices(firstJobId, SLOTS_PER_JOB);
            awaitCompletedCheckpoints(firstJobId, 2L);

            Assertions.assertEquals(
                    FIRST_CHECKPOINT_ID,
                    completedCheckpointIds(firstJobId).get(0),
                    "a fresh job must start its checkpoint sequence at the first id");

            // ---- Phase 2: a second job takes the remaining slices of the same worker.
            siblingJob =
                    submit(
                            engineClient,
                            SLOW_CHECKPOINT_CONF,
                            "sibling-isolation-second",
                            seaTunnelConfig);
            long siblingJobId = siblingJob.getJobId();

            awaitStatus(coordinatorService, siblingJobId, JobStatus.RUNNING);
            awaitBothJobsOwnSlices(firstJobId, siblingJobId);
            awaitCompletedCheckpoints(siblingJobId, 1L);

            Set<Integer> siblingSlicesBeforeCancel = slicesOwnedBy(siblingJobId);
            long siblingCompletedBeforeCancel = completedCheckpoints(siblingJobId);
            long siblingRestoredBeforeCancel = restoredPipelines(siblingJobId);
            long siblingFailedBeforeCancel = failedCheckpoints(siblingJobId);

            // ---- Phase 3: cancel the first job while the sibling's slow checkpoint is in flight.
            // Waiting for an in-flight checkpoint makes the interesting window deterministic: the
            // contract is that this checkpoint survives the cancellation, not that the cancel
            // merely happened to fall between two of the sibling's checkpoints.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            inProgressCheckpoints(siblingJobId) > 0,
                                            "the sibling should have an in-flight checkpoint "
                                                    + "before the co-located job is cancelled"));

            firstJob.cancelJob();
            awaitStatus(coordinatorService, firstJobId, JobStatus.CANCELED);

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Releasing the cancelled job must not move the sibling's slices.
                                Assertions.assertEquals(
                                        siblingSlicesBeforeCancel,
                                        slicesOwnedBy(siblingJobId),
                                        "the surviving job must keep exactly the slices it owned");
                                assertSlicesReleased(firstJobId);
                                Assertions.assertEquals(
                                        SLOTS_PER_JOB,
                                        assignedSlicesOfAllJobs(),
                                        "only the cancelled job's slices may be freed");
                            });

            // ---- Phase 4: the sibling keeps checkpointing across the cancellation.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            completedCheckpoints(siblingJobId)
                                                    > siblingCompletedBeforeCancel,
                                            "the surviving job must keep completing checkpoints "
                                                    + "after its co-located job was cancelled"));

            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(siblingJobId),
                                        "the surviving job must stay RUNNING");
                                Assertions.assertEquals(
                                        siblingRestoredBeforeCancel,
                                        restoredPipelines(siblingJobId),
                                        "cancelling a co-located job must not restore this job");
                                Assertions.assertEquals(
                                        siblingFailedBeforeCancel,
                                        failedCheckpoints(siblingJobId),
                                        "cancelling a co-located job must not fail this job's "
                                                + "checkpoints");
                                Assertions.assertEquals(
                                        siblingSlicesBeforeCancel,
                                        slicesOwnedBy(siblingJobId),
                                        "the surviving job must keep its slices during the "
                                                + "stability window");
                            });

            Assertions.assertTrue(
                    isStrictlyIncreasing(completedCheckpointIds(siblingJobId)),
                    "the surviving job's checkpoint ids must stay unique and increasing: "
                            + completedCheckpointIds(siblingJobId));
            // Fixture self-check: the sibling's sink really does slow every checkpoint down, so the
            // assertions above were made against a checkpoint that was genuinely in flight.
            Assertions.assertTrue(
                    maxCompletedCheckpointDurationMillis(siblingJobId)
                            >= SIBLING_CHECKPOINT_SLEEP_MILLIS,
                    "the sibling's checkpoints should be slow (fixture self-check), saw "
                            + maxCompletedCheckpointDurationMillis(siblingJobId)
                            + " ms");

            // ---- Phase 5: the freed slices are reusable while the sibling keeps running.
            long siblingCompletedBeforeReplacement = completedCheckpoints(siblingJobId);
            replacementJob =
                    submit(
                            engineClient,
                            FAST_CHECKPOINT_CONF,
                            "sibling-isolation-third",
                            seaTunnelConfig);
            long replacementJobId = replacementJob.getJobId();

            awaitStatus(coordinatorService, replacementJobId, JobStatus.RUNNING);
            awaitBothJobsOwnSlices(siblingJobId, replacementJobId);
            awaitCompletedCheckpoints(replacementJobId, 1L);

            Map<Long, Set<Integer>> slicesByOwner = slicesByJob();
            Assertions.assertEquals(
                    siblingSlicesBeforeCancel,
                    slicesByOwner.get(siblingJobId),
                    "the surviving job must keep its exact slices after the replacement starts");
            Assertions.assertTrue(
                    Collections.disjoint(
                            slicesByOwner.get(siblingJobId), slicesByOwner.get(replacementJobId)),
                    "two running jobs must never share a fixed slice: " + slicesByOwner);
            Assertions.assertEquals(
                    FIRST_CHECKPOINT_ID,
                    completedCheckpointIds(replacementJobId).get(0),
                    "the replacement job must start its own checkpoint sequence, not continue the "
                            + "cancelled job's counter");
            // The surviving job checkpoints on its own (slow) cadence, so wait for its next
            // completion instead of sampling once: nothing about the replacement may stop it.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        completedCheckpoints(siblingJobId)
                                                > siblingCompletedBeforeReplacement,
                                        "the surviving job must keep checkpointing while the "
                                                + "replacement runs");
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(siblingJobId),
                                        "the surviving job must stay RUNNING");
                                Assertions.assertEquals(
                                        siblingRestoredBeforeCancel,
                                        restoredPipelines(siblingJobId),
                                        "starting a replacement job must not restore the surviving "
                                                + "job");
                                Assertions.assertEquals(
                                        siblingFailedBeforeCancel,
                                        failedCheckpoints(siblingJobId),
                                        "starting a replacement job must not fail the surviving "
                                                + "job's checkpoints");
                                Assertions.assertEquals(
                                        siblingSlicesBeforeCancel,
                                        slicesOwnedBy(siblingJobId),
                                        "the surviving job must keep its slices while the "
                                                + "replacement runs");
                            });

            // ---- Phase 6: every job ends and the whole fixed pool becomes free again.
            siblingJob.cancelJob();
            replacementJob.cancelJob();
            awaitStatus(coordinatorService, siblingJobId, JobStatus.CANCELED);
            awaitStatus(coordinatorService, replacementJobId, JobStatus.CANCELED);

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                WorkerProfile profile = workerProfile();
                                Assertions.assertEquals(
                                        0,
                                        profile.getAssignedSlots().length,
                                        "no job may keep a slice after every job ended: "
                                                + slicesByJob());
                                Assertions.assertEquals(
                                        SLOT_NUM,
                                        profile.getUnassignedSlots().length,
                                        "the whole fixed pool must be unassigned again");
                            });
        } finally {
            cancelQuietly(firstJob);
            cancelQuietly(siblingJob);
            cancelQuietly(replacementJob);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient,
            String confFile,
            String jobName,
            SeaTunnelConfig seaTunnelConfig)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment executionEnvironment =
                engineClient.createExecutionContext(
                        TestUtils.getResource(confFile), jobConfig, seaTunnelConfig);
        return executionEnvironment.execute();
    }

    private static void cancelQuietly(ClientJobProxy jobProxy) {
        if (jobProxy == null) {
            return;
        }
        try {
            jobProxy.cancelJob();
        } catch (Throwable e) {
            log.info(
                    "Ignoring failure while cancelling job {} during cleanup",
                    jobProxy.getJobId(),
                    e);
        }
    }

    private static void awaitStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expected) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        coordinatorService.getJobStatus(jobId),
                                        "status of job " + jobId));
    }

    private WorkerProfile workerProfile() {
        return resourceManager.getRegisterWorker().get(workerAddress);
    }

    private Map<Long, Set<Integer>> slicesByJob() {
        Map<Long, Set<Integer>> slices = new TreeMap<>();
        for (SlotProfile slot : workerProfile().getAssignedSlots()) {
            slices.computeIfAbsent(slot.getOwnerJobID(), ignored -> new TreeSet<>())
                    .add(slot.getSlotID());
        }
        return slices;
    }

    private Set<Integer> slicesOwnedBy(long jobId) {
        return slicesByJob().getOrDefault(jobId, Collections.emptySet());
    }

    private int assignedSlicesOfAllJobs() {
        return slicesByJob().values().stream().mapToInt(Set::size).sum();
    }

    private void assertSlicesReleased(long jobId) {
        Map<Long, Set<Integer>> slices = slicesByJob();
        Assertions.assertFalse(
                slices.containsKey(jobId),
                "the cancelled job " + jobId + " must own no slice, current owners: " + slices);
    }

    private void awaitJobOwnsSlices(long jobId, int expectedCount) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedCount,
                                        slicesOwnedBy(jobId).size(),
                                        "slices owned by job " + jobId + ": " + slicesByJob()));
    }

    private void awaitBothJobsOwnSlices(long firstJobId, long secondJobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<Long, Set<Integer>> slices = slicesByJob();
                            Assertions.assertEquals(
                                    SLOTS_PER_JOB,
                                    slices.getOrDefault(firstJobId, Collections.emptySet()).size(),
                                    "slices per job: " + slices);
                            Assertions.assertEquals(
                                    SLOTS_PER_JOB,
                                    slices.getOrDefault(secondJobId, Collections.emptySet()).size(),
                                    "slices per job: " + slices);
                            Assertions.assertTrue(
                                    Collections.disjoint(
                                            slices.get(firstJobId), slices.get(secondJobId)),
                                    "two running jobs must never share a fixed slice: " + slices);
                        });
    }

    private void awaitCompletedCheckpoints(long jobId, long expectedAtLeast) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completedCheckpoints(jobId) >= expectedAtLeast,
                                        "completed checkpoints of job "
                                                + jobId
                                                + ": "
                                                + completedCheckpoints(jobId)));
    }

    private long completedCheckpoints(long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(pipeline -> pipeline.getCounts().getCompleted())
                                        .sum())
                .orElse(0L);
    }

    private long inProgressCheckpoints(long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(pipeline -> pipeline.getCounts().getInProgress())
                                        .sum())
                .orElse(0L);
    }

    private long maxCompletedCheckpointDurationMillis(long jobId) {
        return checkpointMonitor.getHistory(jobId, null, HISTORY_LIMIT, CheckpointStatus.COMPLETED)
                .stream()
                .map(entry -> entry.getCheckpointInfo().getDurationMillis())
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
    }

    private long restoredPipelines(long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(pipeline -> pipeline.getCounts().getRestored())
                                        .sum())
                .orElse(0L);
    }

    private long failedCheckpoints(long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(pipeline -> pipeline.getCounts().getFailed())
                                        .sum())
                .orElse(0L);
    }

    private List<Long> completedCheckpointIds(long jobId) {
        return checkpointMonitor.getHistory(jobId, null, HISTORY_LIMIT, CheckpointStatus.COMPLETED)
                .stream()
                .map(entry -> entry.getCheckpointInfo().getCheckpointId())
                .sorted()
                .collect(Collectors.toList());
    }

    private static boolean isStrictlyIncreasing(List<Long> checkpointIds) {
        for (int index = 1; index < checkpointIds.size(); index++) {
            if (checkpointIds.get(index) <= checkpointIds.get(index - 1)) {
                return false;
            }
        }
        return true;
    }
}

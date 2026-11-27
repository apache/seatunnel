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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage for the byte-level accounting of the fixed-slot pool across job lifecycles.
 *
 * <p><b>The contract.</b> With {@code dynamic-slot = false} the worker partitions its max heap into
 * exactly {@code slot-num} equally sized fixed slot profiles and only the integer-division
 * remainder of that partition stays permanently unassigned. Running a job therefore has to move one
 * slice out of {@code unassignedResource} for every slot it owns, and terminating the job (either
 * {@code CANCELED} or {@code FINISHED}) has to put the exact same bytes back:
 *
 * <ol>
 *   <li>before any job the worker advertises {@code slot-num} free slots, no assigned slot, and its
 *       whole max heap as unassigned resource;
 *   <li>a job that owns every fixed slot reports exactly that many assigned slots and leaves only
 *       {@code maxHeap - slotNum * (maxHeap / slotNum)} bytes unassigned, so the pool is a hard
 *       byte-accounted ceiling, not an estimate;
 *   <li>cancelling the job restores the baseline exactly: every slot free again, the whole max heap
 *       back as unassigned resource, no slot ownership left and no job master retained;
 *   <li>the same holds for a job that finishes normally, not only for a cancelled one;
 *   <li>repeating the lifecycle does not drift. Because the fixed pool is inelastic, one missing
 *       slice per cycle permanently shrinks the cluster until a worker restart, with no error
 *       reported to the job that caused it.
 * </ol>
 *
 * <p><b>Why this is a boundary with real regression risk.</b> The accounting is spread over three
 * places that must stay symmetric: {@code DefaultSlotService#initFixedSlots} slices the heap,
 * {@code requestSlot} subtracts the owned profile, and {@code releaseSlot} merges it back on every
 * terminal path. A double subtract or a skipped merge does not fail the job that caused it; it
 * silently makes a later job {@code PENDING} forever under the default {@code WAIT} strategy, which
 * is exactly the capacity leak that is hardest to diagnose in production.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SeaTunnelSlotIT} only counts
 * assigned/unassigned slots for a single job and its release, never the advertised heap; {@code
 * SplitClusterPendingJobLifecycleFailoverIT#testPendingJobScheduledAfterRunningJobCanceled} reuses
 * the slots once but does not assert resource bytes; {@code DynamicSlotNoCapacityCheckIT} is the
 * dynamic-slot counterpart and asserts that the advertised heap deliberately does <em>not</em> move
 * there (every per-task profile is 0/0). No existing test drives several fixed-slot lifecycles
 * through both terminal paths and compares the advertised capacity with the exact baseline after
 * each one.
 */
public class FixedSlotCapacityAccountingIT {

    private static final String STREAMING_CONF = "pending_jobs_streaming.conf";
    private static final String BATCH_CONF = "batch_fake_to_console_minimal_slot.conf";

    /**
     * The streaming fixture needs one slot per task group and the cluster has exactly that many.
     */
    private static final int FIXED_SLOT_NUM = 4;

    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120;
    private static final long CAPACITY_TIMEOUT_SECONDS = 60;

    @Test
    public void testFixedSlotCapacityIsFullyRestoredAcrossJobLifecycles() throws Exception {
        String testClusterName =
                "FixedSlotCapacityAccountingIT"
                        + "_testFixedSlotCapacityIsFullyRestoredAcrossJobLifecycles";
        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ExecutorService clientCalls = null;
        ClientJobProxy streamingJob = null;
        ClientJobProxy batchJob = null;
        ClientJobProxy secondStreamingJob = null;

        try {
            SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
            configureFixedSlotWaitCluster(seaTunnelConfig);
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            final HazelcastInstanceImpl finalNode = node;

            final long maxHeap = Runtime.getRuntime().maxMemory();
            final long slotHeap = maxHeap / FIXED_SLOT_NUM;
            final long remainder = maxHeap - (long) FIXED_SLOT_NUM * slotHeap;

            // Wait for the worker to register before capturing the baseline: registerWorker is
            // filled from the worker's own profile, not from the job submission path.
            Awaitility.await()
                    .atMost(CAPACITY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertBaseline(
                                            currentWorkerProfile(finalNode),
                                            maxHeap,
                                            slotHeap,
                                            remainder));

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            clientCalls =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "fixed-slot-accounting-call");
                                thread.setDaemon(true);
                                return thread;
                            });

            // Lifecycle 1: a streaming job owns the whole pool, then is cancelled.
            streamingJob =
                    submitJob(
                            engineClient,
                            seaTunnelConfig,
                            "fixed_slot_accounting_streaming_1",
                            TestUtils.getResource(STREAMING_CONF));
            assertJobStatusWithTimeout(streamingJob, JobStatus.RUNNING);
            long firstStreamingJobId = streamingJob.getJobId();
            assertPoolConsumed(finalNode, firstStreamingJobId, slotHeap, remainder);

            streamingJob.cancelJob();
            assertCompletion(streamingJob, JobStatus.CANCELED, clientCalls);
            assertPoolRestored(finalNode, firstStreamingJobId, maxHeap);

            // Lifecycle 2: a batch job finishes on its own, exercising the FINISHED release path
            // instead of the cancel path.
            batchJob =
                    submitJob(
                            engineClient,
                            seaTunnelConfig,
                            "fixed_slot_accounting_batch",
                            TestUtils.getResource(BATCH_CONF));
            assertCompletion(batchJob, JobStatus.FINISHED, clientCalls);
            assertPoolRestored(finalNode, batchJob.getJobId(), maxHeap);

            // Lifecycle 3: back to the streaming footprint. The baseline has to be reachable again
            // after a mixed sequence, which is where a per-lifecycle drift would show up.
            secondStreamingJob =
                    submitJob(
                            engineClient,
                            seaTunnelConfig,
                            "fixed_slot_accounting_streaming_2",
                            TestUtils.getResource(STREAMING_CONF));
            assertJobStatusWithTimeout(secondStreamingJob, JobStatus.RUNNING);
            assertPoolConsumed(finalNode, secondStreamingJob.getJobId(), slotHeap, remainder);

            secondStreamingJob.cancelJob();
            assertCompletion(secondStreamingJob, JobStatus.CANCELED, clientCalls);
            assertPoolRestored(finalNode, secondStreamingJob.getJobId(), maxHeap);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        } finally {
            if (clientCalls != null) {
                clientCalls.shutdownNow();
            }
            if (engineClient != null) {
                cancelQuietly(secondStreamingJob);
                cancelQuietly(batchJob);
                cancelQuietly(streamingJob);
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static void assertBaseline(
            WorkerProfile baseline, long maxHeap, long slotHeap, long remainder) {
        Assertions.assertEquals(
                maxHeap,
                baseline.getUnassignedResource().getHeapMemory().getBytes(),
                "A fresh fixed-slot worker must advertise its whole max heap as unassigned");
        Assertions.assertEquals(
                FIXED_SLOT_NUM,
                baseline.getUnassignedSlots().length,
                "A fresh fixed-slot worker must advertise every configured slot as free");
        Assertions.assertEquals(
                0, baseline.getAssignedSlots().length, "No slot may be assigned before any job");
        for (SlotProfile slot : baseline.getUnassignedSlots()) {
            Assertions.assertEquals(
                    slotHeap,
                    slot.getResourceProfile().getHeapMemory().getBytes(),
                    "Every fixed slot must be an equal heap slice: maxHeap / slotNum");
        }
        Assertions.assertEquals(
                maxHeap % FIXED_SLOT_NUM,
                remainder,
                "The permanently unassigned bytes must be exactly the integer-division remainder");
    }

    /**
     * While the job owns the whole pool, the slot arrays, the advertised heap and the persisted
     * slot ownership must all show exactly the configured number of equal heap slices.
     */
    private static void assertPoolConsumed(
            HazelcastInstanceImpl node, long jobId, long slotHeap, long remainder) {
        Awaitility.await()
                .atMost(CAPACITY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            WorkerProfile profile = currentWorkerProfile(node);
                            Assertions.assertEquals(
                                    FIXED_SLOT_NUM,
                                    profile.getAssignedSlots().length,
                                    "A job holding the whole pool must have every slot assigned");
                            Assertions.assertEquals(
                                    0,
                                    profile.getUnassignedSlots().length,
                                    "A job holding the whole pool must leave no free slot");
                            Assertions.assertEquals(
                                    remainder,
                                    profile.getUnassignedResource().getHeapMemory().getBytes(),
                                    "Only the integer-division remainder may stay unassigned");
                        });

        Awaitility.await()
                .atMost(CAPACITY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            long ownedSlots =
                                    ownedSlotProfilesOf(node, jobId).values().stream()
                                            .mapToLong(Map::size)
                                            .sum();
                            Assertions.assertEquals(
                                    FIXED_SLOT_NUM,
                                    ownedSlots,
                                    "The job must record exactly one owned slot per fixed slot");
                            long ownedHeap =
                                    ownedSlotProfilesOf(node, jobId).values().stream()
                                            .flatMap(slots -> slots.values().stream())
                                            .mapToLong(
                                                    slot ->
                                                            slot.getResourceProfile()
                                                                    .getHeapMemory()
                                                                    .getBytes())
                                            .sum();
                            Assertions.assertEquals(
                                    (long) FIXED_SLOT_NUM * slotHeap,
                                    ownedHeap,
                                    "Owned slot profiles must carry exactly the fixed slices");
                        });
    }

    /**
     * A terminal job has to return the pool to the exact baseline, leave no slot ownership behind,
     * and have released its job master so the capacity really is available again.
     */
    private static void assertPoolRestored(HazelcastInstanceImpl node, long jobId, long maxHeap) {
        Awaitility.await()
                .atMost(CAPACITY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            WorkerProfile profile = currentWorkerProfile(node);
                            Assertions.assertEquals(
                                    0,
                                    profile.getAssignedSlots().length,
                                    "A terminal job must not keep any slot assigned");
                            Assertions.assertEquals(
                                    FIXED_SLOT_NUM,
                                    profile.getUnassignedSlots().length,
                                    "A terminal job must return every slot to the free pool");
                            Assertions.assertEquals(
                                    maxHeap,
                                    profile.getUnassignedResource().getHeapMemory().getBytes(),
                                    "A terminal job must restore the whole max heap as unassigned");
                        });

        Awaitility.await()
                .atMost(CAPACITY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        ownedSlotProfilesOf(node, jobId).isEmpty(),
                                        "A terminal job must not leave slot ownership behind"));

        Awaitility.await()
                .atMost(CAPACITY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        getCoordinatorService(node).getJobMaster(jobId),
                                        "The coordinator must release the job master of a terminal job"));
    }

    private static Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesOf(
            HazelcastInstanceImpl node, long jobId) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> owned =
                new java.util.HashMap<>();
        ownedSlotProfiles
                .entrySet()
                .forEach(
                        entry -> {
                            if (entry.getKey().getJobId() == jobId) {
                                owned.put(entry.getKey(), entry.getValue());
                            }
                        });
        return owned;
    }

    private static WorkerProfile currentWorkerProfile(HazelcastInstanceImpl node) {
        WorkerProfile profile = getResourceManager(node).getRegisterWorker().get(node.node.address);
        Assertions.assertNotNull(
                profile, "The local worker must be registered with the resource manager");
        return profile;
    }

    private static void assertCompletion(
            ClientJobProxy job, JobStatus expected, ExecutorService clientCalls) throws Exception {
        Future<JobResult> completion = clientCalls.submit(job::waitForJobCompleteV2);
        JobResult result = completion.get(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertEquals(
                expected,
                result.getStatus(),
                "Unexpected terminal state for job " + job.getJobId());
        Assertions.assertEquals(
                expected,
                job.getJobStatus(),
                "The client view must agree with the completion result");
    }

    private static void cancelQuietly(ClientJobProxy job) {
        if (job == null) {
            return;
        }
        try {
            job.cancelJob();
        } catch (Exception ignored) {
            // best-effort cleanup: the cluster is shut down right after this call
        }
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        return seaTunnelConfig;
    }

    private static void configureFixedSlotWaitCluster(SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(FIXED_SLOT_NUM);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String jobName,
            String jobConfigFile)
            throws ExecutionException, InterruptedException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig)
                .execute();
    }

    private static void assertJobStatusWithTimeout(ClientJobProxy job, JobStatus expected) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, job.getJobStatus()));
    }

    private static ResourceManager getResourceManager(HazelcastInstanceImpl node) {
        return getCoordinatorService(node).getResourceManager();
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }
}

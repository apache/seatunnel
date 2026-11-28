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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.e2e.failurecapacity.ArmableFailingSinkWriter;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * End-to-end regression coverage for what the Zeta scheduler/state machine must guarantee when a
 * job fails permanently: the pipeline-level retry budget is consumed exactly, the job ends in a
 * stable {@code FAILED} terminal state, the whole fixed-slot pool it held becomes allocatable
 * again, and the checkpoint data that makes an operator-driven restore possible is kept.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code FAILED} is the terminal path
 * operators hit most often, and it is reached from the restore machinery rather than from a plain
 * cancel: every failed attempt releases the job's slots and re-applies them for the next attempt,
 * so a single mistake in the release/re-apply bookkeeping either leaks fixed-slot capacity (the
 * cluster silently shrinks and later jobs stay PENDING) or drops the retained checkpoint (the job
 * cannot be recovered after the failure). The retry budget itself is also part of the contract: one
 * attempt too few abandons a recoverable job, one attempt too many turns a deterministic failure
 * into an endless retry loop. All three properties are only observable on a real cluster with a
 * real fixed-slot pool, real task deployments and real checkpoint files.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code
 * JobClientJobProxyIT#testJobRetryTimes} asserts the retry log lines of a containerised cluster and
 * never looks at the eventual terminal state or the slot pool. {@code
 * SplitClusterTerminalZombieJobMasterSwitchIT} produces a {@code FAILED} job with {@code
 * job.retry.times = 0} and asserts checkpoint retention across a master switch, not capacity
 * accounting or the retry budget. The capacity E2Es ({@code SeaTunnelSlotIT} and the fixed-slot
 * accounting tests) only drive jobs through the {@code CANCELED} and {@code FINISHED} paths. No
 * existing test drives a job through several real restore attempts and then checks that the pool is
 * byte-for-byte back to its pre-job state.
 *
 * <p><b>How the failure is produced.</b> The streaming job uses {@code job.retry.times = 2}, so it
 * must be attempted three times (the initial deployment plus two pipeline restores). The test-only
 * {@code ArmableFailingSink} accepts rows while it is not armed, which lets the pipeline reach a
 * steady {@code RUNNING} state and complete a real checkpoint, and fails every row once the test
 * arms it. {@code ArmableFailingSinkWriter#WRITER_INSTANCES} counts deployments, which is the
 * observable retry budget.
 */
@Slf4j
public class PermanentFailureCapacityReleaseIT {

    private static final String CONF_FILE =
            "permanent-failure/stream_fake_to_armable_failing_sink.conf";
    private static final String JOB_NAME = "PermanentFailureCapacityReleaseIT";

    private static final int SLOT_NUM = 4;

    /** {@code job.retry.times} in the job fixture: two restores, i.e. three deployments. */
    private static final int EXPECTED_DEPLOYMENTS = 3;

    private static final long STATE_TIMEOUT_SECONDS = 180;

    /**
     * A terminal job must stay terminal: after this window the coordinator must not have started
     * another deployment of the same job.
     */
    private static final long TERMINAL_STABILITY_SECONDS = 10;

    @Test
    public void testPermanentFailureReleasesFixedSlotsAndKeepsCheckpoint() throws Exception {
        String testClusterName = "PermanentFailureCapacityReleaseIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        // A fixed pool makes capacity observable and keeps a leaked slot from being masked by
        // dynamic allocation.
        clusterConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        clusterConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            SeaTunnelServer seaTunnelServer =
                    node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

            // A fresh member advertises the whole pool; this snapshot is the exact state the failed
            // job must return the pool to.
            awaitPoolSlicesUnassigned(seaTunnelServer, SLOT_NUM);
            SlotPoolSnapshot baseline = SlotPoolSnapshot.of(seaTunnelServer);

            Common.setDeployMode(DeployMode.CLIENT);
            String confPath = TestUtils.getResource(CONF_FILE);

            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            ArmableFailingSinkWriter.reset();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(JOB_NAME);
            ClientJobProxy job =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, clusterConfig)
                            .execute();
            long jobId = job.getJobId();

            awaitJobStatus(job, JobStatus.RUNNING);
            JobPipelineCheckpointData checkpoint = awaitFirstCompletedCheckpoint(jobClient, jobId);
            log.info(
                    "Job {} is RUNNING with its first completed checkpoint {}",
                    jobId,
                    checkpoint.getCheckpointId());

            // From here on every row fails, so the current attempt fails, the two retries are
            // consumed and the job must end FAILED.
            ArmableFailingSinkWriter.ARMED.set(true);
            awaitJobStatus(job, JobStatus.FAILED);

            Assertions.assertEquals(
                    EXPECTED_DEPLOYMENTS,
                    ArmableFailingSinkWriter.WRITER_INSTANCES.get(),
                    "job.retry.times=2 must allow exactly two restores, i.e. three deployments");

            // FAILED must be terminal: no further deployment and no state change after a window
            // that is much longer than the job's 1s retry interval.
            TimeUnit.SECONDS.sleep(TERMINAL_STABILITY_SECONDS);
            Assertions.assertEquals(
                    JobStatus.FAILED,
                    job.getJobStatus(),
                    "A permanently failed job must stay FAILED instead of restarting");
            Assertions.assertEquals(
                    EXPECTED_DEPLOYMENTS,
                    ArmableFailingSinkWriter.WRITER_INSTANCES.get(),
                    "A permanently failed job must not deploy another attempt");
            Assertions.assertEquals(
                    JobStatus.FAILED.name(),
                    engineClient.getJobStatus(jobId),
                    "The terminal FAILED state must still be queryable after the coordinator released the job");

            // The whole fixed pool must be allocatable again, not merely reported as free by the
            // slot service: no slice may stay assigned and no ownership record may survive.
            SlotPoolSnapshot afterFailure = awaitPoolRestored(seaTunnelServer, baseline);
            log.info("Pool restored after permanent failure: {}", afterFailure);

            IMap<PipelineLocation, ?> ownedSlotProfiles =
                    node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
            Set<Long> ownedJobIds =
                    ownedSlotProfiles.keySet().stream()
                            .map(PipelineLocation::getJobId)
                            .collect(Collectors.toCollection(TreeSet::new));
            Assertions.assertFalse(
                    ownedJobIds.contains(jobId),
                    "The failed job must not keep any slot ownership record, owned job ids: "
                            + ownedJobIds);

            // FAILED is not FINISHED/CANCELED, so the checkpoint data must survive: it is the only
            // way to recover the job after a permanent failure.
            List<JobPipelineCheckpointData> retainedCheckpoints =
                    jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT);
            Assertions.assertFalse(
                    retainedCheckpoints.isEmpty(),
                    "A permanently failed job must keep its checkpoint data for a later restore");

            // Functional proof that the capacity is usable: a replacement job with the same
            // footprint must be placed immediately and reach RUNNING.
            ArmableFailingSinkWriter.ARMED.set(false);
            JobConfig replacementJobConfig = new JobConfig();
            replacementJobConfig.setName(JOB_NAME + "-replacement");
            ClientJobProxy replacement =
                    engineClient
                            .createExecutionContext(confPath, replacementJobConfig, clusterConfig)
                            .execute();
            long replacementJobId = replacement.getJobId();
            try {
                awaitJobStatus(replacement, JobStatus.RUNNING);
                Assertions.assertTrue(
                        ArmableFailingSinkWriter.WRITER_INSTANCES.get() > EXPECTED_DEPLOYMENTS,
                        "The replacement job must actually be deployed");
            } finally {
                engineClient.cancelJob(replacementJobId);
            }
            awaitJobStatus(replacement, JobStatus.CANCELED);

            // The replacement job's cancellation must hand the pool back as well.
            awaitPoolRestored(seaTunnelServer, baseline);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
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

    private static JobPipelineCheckpointData awaitFirstCompletedCheckpoint(
            JobClient jobClient, long jobId) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        jobClient
                                                .getCheckpointData(jobId, RestoreMode.CHECKPOINT)
                                                .isEmpty(),
                                        "Job " + jobId + " has no completed checkpoint yet"));
        return jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT).get(0);
    }

    private static void awaitPoolSlicesUnassigned(SeaTunnelServer seaTunnelServer, int slotNum) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        slotNum, unassignedSlotIds(seaTunnelServer).size()));
    }

    private static SlotPoolSnapshot awaitPoolRestored(
            SeaTunnelServer seaTunnelServer, SlotPoolSnapshot baseline) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        baseline,
                                        SlotPoolSnapshot.of(seaTunnelServer),
                                        "The fixed-slot pool must return to its pre-job state"));
        return SlotPoolSnapshot.of(seaTunnelServer);
    }

    private static Set<Integer> unassignedSlotIds(SeaTunnelServer seaTunnelServer) {
        WorkerProfile workerProfile = seaTunnelServer.getSlotService().getWorkerProfile();
        SlotProfile[] unassignedSlots = workerProfile.getUnassignedSlots();
        if (unassignedSlots == null) {
            return new TreeSet<>();
        }
        return Arrays.stream(unassignedSlots)
                .map(SlotProfile::getSlotID)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /** Immutable view of the worker's fixed-slot pool: which slices are free and how much heap. */
    private static final class SlotPoolSnapshot {
        private final Set<Integer> unassignedSlotIds;
        private final int assignedSlotCount;
        private final long unassignedHeapBytes;

        private SlotPoolSnapshot(
                Set<Integer> unassignedSlotIds, int assignedSlotCount, long unassignedHeapBytes) {
            this.unassignedSlotIds = unassignedSlotIds;
            this.assignedSlotCount = assignedSlotCount;
            this.unassignedHeapBytes = unassignedHeapBytes;
        }

        static SlotPoolSnapshot of(SeaTunnelServer seaTunnelServer) {
            WorkerProfile workerProfile = seaTunnelServer.getSlotService().getWorkerProfile();
            SlotProfile[] assignedSlots = workerProfile.getAssignedSlots();
            SlotProfile[] unassignedSlots = workerProfile.getUnassignedSlots();
            return new SlotPoolSnapshot(
                    unassignedSlotIds(seaTunnelServer),
                    assignedSlots == null ? 0 : assignedSlots.length,
                    workerProfile.getUnassignedResource().getHeapMemory().getBytes());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SlotPoolSnapshot)) {
                return false;
            }
            SlotPoolSnapshot other = (SlotPoolSnapshot) o;
            return assignedSlotCount == other.assignedSlotCount
                    && unassignedHeapBytes == other.unassignedHeapBytes
                    && unassignedSlotIds.equals(other.unassignedSlotIds);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(
                    unassignedSlotIds, assignedSlotCount, unassignedHeapBytes);
        }

        @Override
        public String toString() {
            return "unassignedSlotIds="
                    + unassignedSlotIds
                    + ", assignedSlotCount="
                    + assignedSlotCount
                    + ", unassignedHeapBytes="
                    + unassignedHeapBytes;
        }
    }
}

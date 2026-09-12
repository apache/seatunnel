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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStatusData;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.master.cleanup.JobCleanupRecord;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Regression test for <a href="https://github.com/apache/seatunnel/issues/10675">#10675</a>, fixed
 * by <a href="https://github.com/apache/seatunnel/pull/10692">#10692</a> ("[Fix][Zeta] Prevent
 * terminal-state zombie jobs from being restored after master switch").
 *
 * <p>The production incident: a CDC job failed during a rolling restart, but the coordinator died
 * after the terminal {@code FAILED} status had been persisted to {@code runningJobStateIMap} and
 * before the job's {@code runningJobInfoIMap} entry had been removed. On the next master switch,
 * {@code CoordinatorService#restoreJobFromMasterActiveSwitch} only checked {@code jobState ==
 * null}, so the terminal job was rebuilt as a brand-new {@link JobMaster}, showed {@code RUNNING}
 * in the REST API for days without ever checkpointing, and was eventually restored from a
 * checkpoint that pointed at a long-purged binlog position. The fix routes every terminal-state
 * entry found during master-switch restore through {@code cleanupTerminalZombieJob(...)} instead of
 * {@code new JobMaster(...)}.
 *
 * <p>Existing coverage and the gap this test closes:
 *
 * <ul>
 *   <li>{@code CoordinatorServiceTest#testTerminalZombieJobShouldNotRestartAfterMasterSwitch}
 *       (shipped with #10692) is a unit test in {@code seatunnel-engine-server}: it hand-injects a
 *       {@code CANCELED} zombie into the IMaps and asserts it is not re-executed.
 *   <li>{@code
 *       SplitClusterPendingJobLifecycleFailoverIT#testTerminalJobCleanupSkipsWorkerWaitAfterMasterSwitch}
 *       (#12034) covers the sibling branch where the delayed-cleanup tombstone record in {@code
 *       pendingJobCleanupIMap} survived the switch, i.e. {@code schedulePendingJobCleanup(...)}.
 *   <li>Nothing exercised the incident's actual branch end to end: a {@code FAILED} job whose
 *       coordinator died before any cleanup bookkeeping was written, restored through the real
 *       multi-node master switch with a live worker registered, observed through the client API,
 *       with the fix's exact cleanup semantics (immediate {@code runningJobInfoIMap} removal, state
 *       key removal, history reconstruction, checkpoint retention for {@code FAILED}) asserted.
 * </ul>
 *
 * <p>How the terminal job is produced: a real unbounded streaming job (see {@code
 * stream_fake_to_console_terminal_zombie_master_switch.conf}) runs on a single worker and
 * checkpoints every second. Once at least one checkpoint has been persisted, that worker is shut
 * down. {@code TaskExecutionService} reports its interrupted tasks as {@code FAILED} (an interrupt
 * without a cancel request is treated as a failure), {@code
 * CoordinatorService#failedTaskOnMemberRemoved} independently fails any task still deployed on the
 * removed member, and {@code job.retry.times = 0} leaves {@code SubPlan#canRestorePipeline()}
 * false, so the pipeline and the job end {@code FAILED} without a restore attempt. Because {@code
 * CheckpointManager#clearCheckpointIfNeed} only deletes checkpoints for {@code FINISHED} and {@code
 * CANCELED}, the failed job leaves its checkpoint on disk, exactly like the incident's job did. A
 * fresh worker is then started so that the cluster has a registered worker at the moment of the
 * master switch.
 *
 * <p>How the zombie is constructed, and why it is equivalent to the incident: once the job is
 * {@code FAILED}, its genuine runtime footprint is left in place: the original {@link JobInfo} in
 * {@code runningJobInfoIMap}, the terminal job, pipeline and task-group states plus their
 * timestamps in {@code runningJobStateIMap} / {@code engine_stateTimestamps}, the checkpoint
 * coordinator's {@code checkpoint_state_*} keys, and the checkpoint files on disk. The test then
 * removes only the bookkeeping that {@code JobMaster#cleanJob()} writes <em>after</em> the terminal
 * state has been persisted: the finished-job history entries ({@code engine_finishedJobState},
 * {@code engine_finishedJobVertexInfo}) and the delayed-cleanup tombstone in {@code
 * engine_pendingJobCleanup}. That is precisely the IMap state a coordinator leaves behind when it
 * dies immediately after {@code PhysicalPlan#updateJobState(FAILED)} and before {@code cleanJob()}
 * completes, which is the window described in #10675. Constructing it this way is deterministic (no
 * race against the coordinator has to be won) and keeps every entry the real job wrote, rather than
 * re-inserting synthetic ones. {@code state-cleanup-delay-ms} is raised far beyond the test's
 * lifetime so the old master's own delayed-cleanup timer cannot touch the zombie, and {@code
 * history-job-expire-minutes} is raised so the reconstructed history cannot expire mid-test.
 *
 * <p>A worker is registered at switch time on purpose: before #10692 the restore loop only ran once
 * a worker had registered, and with one present the zombie would have been rebuilt as a fresh
 * {@link JobMaster} and enqueued for scheduling. With the fix, the terminal entry is cleaned up
 * before the worker-wait loop and no {@link JobMaster} is ever created for it.
 */
@Slf4j
public class SplitClusterTerminalZombieJobMasterSwitchIT {

    private static final String JOB_CONFIG_FILE =
            "stream_fake_to_console_terminal_zombie_master_switch.conf";

    private static final String JOB_NAME = "terminal_zombie_failed_job";

    /** Prefix used by the checkpoint coordinator for its per-pipeline state keys. */
    private static final String CHECKPOINT_STATE_KEY_PREFIX = "checkpoint_state_";

    @Test
    public void testFailedZombieJobIsCleanedInsteadOfRestoredAfterMasterSwitch() throws Exception {
        String testClusterName =
                "SplitClusterTerminalZombieJobMasterSwitchIT_"
                        + "testFailedZombieJobIsCleanedInsteadOfRestoredAfterMasterSwitch";

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            HazelcastInstanceImpl finalMasterNode1 = masterNode1;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMasterNode1.getCluster().getMembers().size()));

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;
            CoordinatorService activeCoordinator = coordinatorService(activeMaster);
            CheckpointStorage checkpointStorage = checkpointStorage(activeMaster);

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            ClientJobProxy clientJobProxy =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            JOB_NAME,
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long jobId = clientJobProxy.getJobId();
            log.info("Submitted terminal-zombie candidate job {}", jobId);

            // The JobMaster exists (pending or running) from the moment submission returns until
            // the job completes, so its physical plan can be captured right away.
            JobMaster jobMaster = activeCoordinator.getJobMaster(jobId);
            Assertions.assertNotNull(jobMaster, "JobMaster must exist right after submission");
            List<TaskGroupLocation> taskGroupLocations = new ArrayList<>();
            List<PipelineLocation> pipelineLocations = new ArrayList<>();
            for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
                pipelineLocations.add(subPlan.getPipelineLocation());
                subPlan.getCoordinatorVertexList()
                        .forEach(vertex -> taskGroupLocations.add(vertex.getTaskGroupLocation()));
                subPlan.getPhysicalVertexList()
                        .forEach(vertex -> taskGroupLocations.add(vertex.getTaskGroupLocation()));
            }
            Assertions.assertFalse(pipelineLocations.isEmpty());
            Assertions.assertFalse(taskGroupLocations.isEmpty());

            // Phase 1: the job runs and persists at least one checkpoint, then loses its only
            // worker and, with job.retry.times = 0, terminates as FAILED.
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertFalse(
                                        listCheckpoints(checkpointStorage, jobId).isEmpty(),
                                        "Waiting for the first persisted checkpoint");
                            });
            CompletableFuture<JobResult> jobResultFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobCompleteV2);

            log.info("Shutting down the only worker to fail job {}", jobId);
            workerNode1.shutdown();
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        jobResultFuture.isDone(),
                                        "Waiting for the worker loss to terminate the job");
                                Assertions.assertEquals(
                                        JobStatus.FAILED, jobResultFuture.get().getStatus());
                            });
            log.info("Job {} reached FAILED: {}", jobId, jobResultFuture.get().getError());

            // A fresh worker that never hosted the job: any task group of the job that shows up on
            // it later can only come from a resurrection, and its presence gives a pre-fix
            // coordinator everything it needed to rebuild the zombie as a live JobMaster.
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);
            SeaTunnelServer workerServer = seaTunnelServer(workerNode2);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1,
                                            activeCoordinator
                                                    .getResourceManager()
                                                    .workerCount(Collections.emptyMap()),
                                            "Replacement worker must register on the master"));

            IMap<Long, JobInfo> runningJobInfoIMap =
                    activeMaster.getMap(Constant.IMAP_RUNNING_JOB_INFO);
            IMap<Object, Object> runningJobStateIMap =
                    activeMaster.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            IMap<Object, Long[]> runningJobStateTimestampsIMap =
                    activeMaster.getMap(Constant.IMAP_STATE_TIMESTAMPS);
            IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                    activeMaster.getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
            IMap<Long, JobHistoryService.JobState> finishedJobStateIMap =
                    activeMaster.getMap(Constant.IMAP_FINISHED_JOB_STATE);
            IMap<Long, JobDAGInfo> finishedJobDagInfoIMap =
                    activeMaster.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);

            // Phase 2: wait until the real terminal footprint is complete on the old master. The
            // tombstone in pendingJobCleanupIMap is the last thing JobMaster#cleanJob() writes, so
            // its presence proves the whole terminal bookkeeping sequence has run.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FAILED, runningJobStateIMap.get(jobId));
                                Assertions.assertTrue(runningJobInfoIMap.containsKey(jobId));
                                Assertions.assertTrue(pendingJobCleanupIMap.containsKey(jobId));
                                Assertions.assertTrue(finishedJobStateIMap.containsKey(jobId));
                                Assertions.assertTrue(finishedJobDagInfoIMap.containsKey(jobId));
                                Assertions.assertNull(
                                        activeCoordinator.getJobMaster(jobId),
                                        "Completed JobMaster must have been released");
                            });

            JobInfo zombieJobInfo = runningJobInfoIMap.get(jobId);
            JobCleanupRecord tombstone = pendingJobCleanupIMap.get(jobId);
            Assertions.assertEquals(JobStatus.FAILED, tombstone.getFinalStatus());
            Assertions.assertEquals(
                    zombieJobInfo.getInitializationTimestamp(),
                    tombstone.getOwnerInitializationTimestamp());
            JobHistoryService.JobState historyBeforeSwitch = finishedJobStateIMap.get(jobId);
            Assertions.assertEquals(JobStatus.FAILED, historyBeforeSwitch.getJobStatus());
            Long[] jobStateTimestamps = runningJobStateTimestampsIMap.get(jobId);
            Assertions.assertNotNull(jobStateTimestamps);
            Assertions.assertNotNull(jobStateTimestamps[JobStatus.FAILED.ordinal()]);
            Set<Object> jobOwnedStateKeys = collectJobOwnedKeys(runningJobStateIMap, jobId);
            Assertions.assertTrue(jobOwnedStateKeys.contains(jobId));
            Assertions.assertTrue(jobOwnedStateKeys.containsAll(pipelineLocations));
            Assertions.assertTrue(jobOwnedStateKeys.containsAll(taskGroupLocations));
            List<PipelineState> checkpointsBeforeSwitch = listCheckpoints(checkpointStorage, jobId);
            Assertions.assertFalse(
                    checkpointsBeforeSwitch.isEmpty(),
                    "A FAILED job must keep its persisted checkpoint");
            Assertions.assertFalse(
                    hasActiveTaskGroup(workerServer, taskGroupLocations),
                    "The replacement worker never hosted the failed job");
            log.info(
                    "Terminal footprint of job {}: stateKeys={}, checkpoints={}",
                    jobId,
                    jobOwnedStateKeys,
                    checkpointsBeforeSwitch.size());

            // Phase 3: construct the incident zombie. Strip exactly the bookkeeping that
            // JobMaster#cleanJob() produces after the terminal state has already been persisted;
            // everything the job itself wrote stays untouched.
            pendingJobCleanupIMap.remove(jobId);
            finishedJobStateIMap.remove(jobId);
            finishedJobDagInfoIMap.remove(jobId);

            Assertions.assertEquals(zombieJobInfo, runningJobInfoIMap.get(jobId));
            Assertions.assertEquals(JobStatus.FAILED, runningJobStateIMap.get(jobId));
            Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
            Assertions.assertFalse(finishedJobStateIMap.containsKey(jobId));
            // With the history gone and no JobMaster, the old master no longer reports the job at
            // all: the only remaining trace is the zombie footprint in the IMaps.
            Assertions.assertEquals(JobStatus.UNKNOWABLE, activeCoordinator.getJobStatus(jobId));

            // Phase 4: master switch with a worker registered, so a pre-fix coordinator would have
            // had everything it needed to rebuild the zombie as a live JobMaster.
            log.info("Shutting down active master to trigger master switch");
            activeMaster.shutdown();

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        standbyMaster.getLifecycleService().isRunning());
                                Assertions.assertEquals(
                                        2, standbyMaster.getCluster().getMembers().size());
                                Assertions.assertTrue(
                                        isCoordinatorActive(standbyMaster),
                                        "Standby master should become active after failover");
                            });
            CoordinatorService standbyCoordinator = coordinatorService(standbyMaster);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1,
                                            standbyCoordinator
                                                    .getResourceManager()
                                                    .workerCount(Collections.emptyMap()),
                                            "Worker must be registered on the new master"));

            IMap<Long, JobInfo> runningJobInfoOnStandby =
                    standbyMaster.getMap(Constant.IMAP_RUNNING_JOB_INFO);
            IMap<Object, Object> runningJobStateOnStandby =
                    standbyMaster.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            IMap<Object, Long[]> runningJobStateTimestampsOnStandby =
                    standbyMaster.getMap(Constant.IMAP_STATE_TIMESTAMPS);
            IMap<Long, JobCleanupRecord> pendingJobCleanupOnStandby =
                    standbyMaster.getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
            CheckpointStorage standbyCheckpointStorage = checkpointStorage(standbyMaster);

            // Phase 5: the fix's cleanupTerminalZombieJob(...) semantics, exactly as implemented.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        runningJobInfoOnStandby.containsKey(jobId),
                                        "Zombie entry must be removed from runningJobInfoIMap");
                                Assertions.assertEquals(
                                        Collections.emptySet(),
                                        collectJobOwnedKeys(runningJobStateOnStandby, jobId),
                                        "All job-owned state keys must be removed");
                                Assertions.assertEquals(
                                        Collections.emptySet(),
                                        collectJobOwnedKeys(
                                                runningJobStateTimestampsOnStandby, jobId),
                                        "All job-owned timestamp keys must be removed");
                                Assertions.assertFalse(
                                        pendingJobCleanupOnStandby.containsKey(jobId),
                                        "Immediate zombie cleanup must not leave a tombstone");
                                Assertions.assertNull(
                                        standbyCoordinator.getJobMaster(jobId),
                                        "No JobMaster may be created for a terminal zombie");
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        standbyCoordinator.getJobStatus(jobId),
                                        "Terminal status must be reported from rebuilt history");
                            });

            // History is reconstructed from the zombie footprint before the state keys are removed,
            // so the rebuilt entry must carry the real job name and the real state timestamps.
            JobHistoryService.JobState rebuiltHistory =
                    standbyCoordinator.getJobHistoryService().getJobDetailState(jobId);
            Assertions.assertNotNull(rebuiltHistory);
            Assertions.assertEquals(JobStatus.FAILED, rebuiltHistory.getJobStatus());
            Assertions.assertEquals(JOB_NAME, rebuiltHistory.getJobName());
            Assertions.assertEquals(
                    historyBeforeSwitch.getSubmitTime(), rebuiltHistory.getSubmitTime());
            Assertions.assertEquals(
                    historyBeforeSwitch.getStartTime(), rebuiltHistory.getStartTime());
            Assertions.assertEquals(
                    jobStateTimestamps[JobStatus.FAILED.ordinal()], rebuiltHistory.getFinishTime());
            Assertions.assertNotNull(
                    standbyCoordinator.getJobHistoryService().getJobDAGInfo(jobId),
                    "DAG info must be rebuilt for the terminal zombie");

            // The client-visible surface (the one that showed RUNNING for days in #10675): the job
            // is listed exactly once, as FAILED, and the client status query agrees.
            List<JobStatusData> listedStates =
                    standbyCoordinator.getJobHistoryService().getJobStatusData().stream()
                            .filter(jobStatusData -> jobStatusData.getJobId() == jobId)
                            .collect(Collectors.toList());
            Assertions.assertEquals(1, listedStates.size(), "Job must be listed exactly once");
            Assertions.assertEquals(JobStatus.FAILED, listedStates.get(0).getJobStatus());
            Assertions.assertFalse(standbyCoordinator.shouldShowAsRunningJob(jobId));
            Assertions.assertEquals(JobStatus.FAILED, clientJobProxy.getJobStatus());

            // Phase 6: hold the outcome for a stability window. A resurrected job would have to
            // show up here as a JobMaster, a pending-queue entry, task groups deployed on the
            // worker, or a status change; and the FAILED job's checkpoint must survive, because
            // cleanupTerminalZombieCheckpointIfNecessary only deletes for FINISHED/CANCELED.
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertNull(standbyCoordinator.getJobMaster(jobId));
                                Assertions.assertFalse(
                                        standbyCoordinator.getPendingJobQueue().contains(jobId));
                                Assertions.assertEquals(
                                        JobStatus.FAILED, standbyCoordinator.getJobStatus(jobId));
                                Assertions.assertFalse(
                                        hasActiveTaskGroup(workerServer, taskGroupLocations),
                                        "Terminal zombie must never be re-deployed on the worker");
                                Assertions.assertFalse(runningJobInfoOnStandby.containsKey(jobId));
                                Assertions.assertEquals(
                                        checkpointsBeforeSwitch.size(),
                                        listCheckpoints(standbyCheckpointStorage, jobId).size(),
                                        "Checkpoint of a FAILED zombie must be retained");
                            });
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode1 != null && workerNode1.getLifecycleService().isRunning()) {
                workerNode1.shutdown();
            }
            if (workerNode2 != null && workerNode2.getLifecycleService().isRunning()) {
                workerNode2.shutdown();
            }
            if (masterNode1 != null && masterNode1.getLifecycleService().isRunning()) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null && masterNode2.getLifecycleService().isRunning()) {
                masterNode2.shutdown();
            }
        }
    }

    /**
     * Builds a node config for the test cluster. The delayed-cleanup window and the history TTL are
     * both pushed far beyond the test's lifetime so that neither the old master's cleanup timer nor
     * history expiry can interfere with what the assertions observe.
     */
    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        seaTunnelConfig.getEngineConfig().setStateCleanupDelayMillis(TimeUnit.MINUTES.toMillis(10));
        seaTunnelConfig.getEngineConfig().setHistoryJobExpireMinutes(30);
        return seaTunnelConfig;
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String jobName,
            String jobConfigFile) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig);
        try {
            return jobExecutionEnv.execute();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to submit job " + jobName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted when submitting job " + jobName, e);
        }
    }

    private static SeaTunnelServer seaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    private static CoordinatorService coordinatorService(HazelcastInstanceImpl masterNode) {
        return seaTunnelServer(masterNode).getCoordinatorService();
    }

    private static CheckpointStorage checkpointStorage(HazelcastInstanceImpl masterNode) {
        return seaTunnelServer(masterNode).getCheckpointService().getCheckpointStorage();
    }

    /**
     * Lists the persisted checkpoints of a job. Storage errors are surfaced as assertion failures
     * so that callers inside Awaitility keep polling instead of aborting on a transient exception.
     */
    private static List<PipelineState> listCheckpoints(CheckpointStorage storage, long jobId) {
        try {
            return storage.getAllCheckpoints(String.valueOf(jobId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail("Failed to list checkpoints of job " + jobId, e);
        }
    }

    /**
     * Mirrors the key ownership rule of {@code CoordinatorService#belongsToJob}: a job owns its own
     * id, its pipeline and task-group locations, and the checkpoint coordinator's {@code
     * checkpoint_state_<jobId>_*} entries.
     */
    private static Set<Object> collectJobOwnedKeys(IMap<Object, ?> map, long jobId) {
        return map.keySet().stream()
                .filter(key -> belongsToJob(key, jobId))
                .collect(Collectors.toSet());
    }

    private static boolean belongsToJob(Object key, long jobId) {
        if (key instanceof Long) {
            return (Long) key == jobId;
        }
        if (key instanceof PipelineLocation) {
            return ((PipelineLocation) key).getJobId() == jobId;
        }
        if (key instanceof TaskGroupLocation) {
            return ((TaskGroupLocation) key).getJobId() == jobId;
        }
        if (key instanceof String) {
            return ((String) key).startsWith(CHECKPOINT_STATE_KEY_PREFIX + jobId + "_");
        }
        return false;
    }

    /**
     * Returns true when any of the given task groups still has an execution context on the node.
     */
    private static boolean hasActiveTaskGroup(
            SeaTunnelServer server, List<TaskGroupLocation> taskGroupLocations) {
        for (TaskGroupLocation location : taskGroupLocations) {
            try {
                if (server.getTaskExecutionService().getActiveExecutionContext(location) != null) {
                    return true;
                }
            } catch (TaskGroupContextNotFoundException e) {
                // Not deployed on this node: exactly what a cleaned-up terminal job looks like.
            }
        }
        return false;
    }

    private static HazelcastInstanceImpl waitAndFindActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        final HazelcastInstanceImpl[] activeMasterRef = new HazelcastInstanceImpl[1];
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            activeMasterRef[0] = findActiveMaster(masterNode1, masterNode2);
                            Assertions.assertNotNull(
                                    activeMasterRef[0],
                                    "Should find active master after coordinator initialization");
                        });
        return activeMasterRef[0];
    }

    private static HazelcastInstanceImpl findActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        if (isCoordinatorActive(masterNode1)) {
            return masterNode1;
        }
        if (isCoordinatorActive(masterNode2)) {
            return masterNode2;
        }
        return null;
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl masterNode) {
        if (masterNode == null || !masterNode.getLifecycleService().isRunning()) {
            return false;
        }
        try {
            return coordinatorService(masterNode).isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }
}

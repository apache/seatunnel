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
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end regression coverage for <a
 * href="https://github.com/apache/seatunnel/issues/13482">#13482</a>: a Zeta job that reaches a
 * terminal state before any of its pipelines start must clean up the per-pipeline checkpoint-ID
 * counters it initialized during {@code JobMaster.init()}.
 *
 * <p><b>The contract under test.</b> {@code CheckpointManager} allocates one entry in the {@code
 * engine_checkpoint-id-map} IMap for every pipeline as soon as the {@code JobMaster} is created.
 * That map has no TTL, so the entry can only disappear when the pipeline's counter is explicitly
 * shut down. A job that is canceled while it waits in {@code pendingJobQueue} (or that fails
 * immediately under {@code ScheduleStrategy.REJECT} because the cluster has too few slots) never
 * runs a {@code SubPlan}, and that path used to skip the counter shutdown entirely. Every such job
 * then left one permanent entry per pipeline in cluster memory and in the persisted IMap store.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> The leak is invisible from the job
 * status, the checkpoint storage, or the REST API: the job is {@code CANCELED} or {@code FAILED}
 * and the checkpoint files are gone, yet the counter map keeps growing. It only shows up after many
 * short-lived jobs and, when IMap persistence is enabled, it survives restarts. The fix is also
 * easy to regress because the existing counter shutdown is wired to the {@code SubPlan} state
 * machine, which these jobs never enter.
 *
 * <p><b>Why existing coverage does not reach it.</b> {@code CheckpointManagerTest} and {@code
 * StateStoreCheckpointIDCounterTest} exercise the counter directly against a mocked store; they
 * never create a real {@code JobMaster} for a job that is rejected or canceled before scheduling.
 * {@code SeaTunnelSlotIT} asserts the terminal job status and slot release but never inspects the
 * checkpoint-ID map. This test drives the real client-submit path on an embedded cluster, proves
 * the counter exists while the job is {@code PENDING}, and then proves that reaching a terminal
 * state removes it.
 */
@Slf4j
public class CheckpointIdCounterTerminalCleanupIT {

    /**
     * Streaming job used only as a fixture: its source needs two slots and its sink needs one, so a
     * cluster with two fixed slots cannot schedule it. Under {@code WAIT} the job parks in the
     * pending queue; under {@code REJECT} it terminates before any pipeline starts.
     */
    private static final String JOB_CONF = "pending_jobs_streaming.conf";

    private static final long STATE_TIMEOUT_SECONDS = 120;
    private static final long CLEANUP_TIMEOUT_SECONDS = 60;

    @Test
    public void testCanceledPendingJobReleasesCheckpointIdCounters() throws Exception {
        String clusterName =
                TestUtils.getClusterName("CheckpointIdCounterPendingCancelIT")
                        + "_"
                        + System.nanoTime();
        SeaTunnelConfig seaTunnelConfig = buildConfig(clusterName, ScheduleStrategy.WAIT);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            engineClient = buildClient(clusterName);
            job = submitJob(engineClient, seaTunnelConfig, clusterName);

            ClientJobProxy submittedJob = job;
            HazelcastInstanceImpl cluster = node;
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING, submittedJob.getJobStatus());
                                Assertions.assertTrue(
                                        containsCounterForJob(cluster, submittedJob.getJobId()),
                                        "the pending job should have initialized its pipeline"
                                                + " checkpoint-ID counter before being canceled");
                            });

            submittedJob.cancelJob();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED, submittedJob.getJobStatus()));

            awaitNoCounterForJob(cluster, submittedJob.getJobId());
        } finally {
            closeQuietly(engineClient);
            shutdownQuietly(node);
        }
    }

    @Test
    public void testRejectedJobReleasesCheckpointIdCounters() throws Exception {
        String clusterName =
                TestUtils.getClusterName("CheckpointIdCounterRejectedIT") + "_" + System.nanoTime();
        SeaTunnelConfig seaTunnelConfig = buildConfig(clusterName, ScheduleStrategy.REJECT);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            engineClient = buildClient(clusterName);
            job = submitJob(engineClient, seaTunnelConfig, clusterName);

            Assertions.assertEquals(JobStatus.FAILED, job.waitForJobComplete());
            awaitNoCounterForJob(node, job.getJobId());
        } finally {
            closeQuietly(engineClient);
            shutdownQuietly(node);
        }
    }

    private static SeaTunnelConfig buildConfig(
            String clusterName, ScheduleStrategy scheduleStrategy) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        // The REST endpoint is irrelevant here and a disabled endpoint keeps the test isolated
        // from a fixed port that another test may already hold.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        // The fixture job needs three slots, so two fixed slots force the pending/reject paths.
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(2);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(scheduleStrategy);
        return seaTunnelConfig;
    }

    private static SeaTunnelClient buildClient(String clusterName) {
        Common.setDeployMode(DeployMode.CLIENT);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        return new SeaTunnelClient(clientConfig);
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig, String jobName)
            throws ExecutionException, InterruptedException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        String confPath = TestUtils.getResource(JOB_CONF);
        ClientJobExecutionEnvironment executionEnvironment =
                engineClient.createExecutionContext(confPath, jobConfig, seaTunnelConfig);
        return executionEnvironment.execute();
    }

    private static void awaitNoCounterForJob(HazelcastInstanceImpl node, long jobId) {
        Awaitility.await()
                .atMost(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        containsCounterForJob(node, jobId),
                                        "checkpoint-ID counters for terminal job "
                                                + jobId
                                                + " were not removed from "
                                                + EngineStateStoreNames.CHECKPOINT_ID));
    }

    private static boolean containsCounterForJob(HazelcastInstanceImpl node, long jobId) {
        IMap<String, Long> checkpointIdCounters = node.getMap(EngineStateStoreNames.CHECKPOINT_ID);
        for (String key : checkpointIdCounters.keySet()) {
            long[] jobAndPipeline = StateStoreCheckpointIDCounter.convertBase64ToLongInt(key);
            if (jobAndPipeline[0] == jobId) {
                return true;
            }
        }
        return false;
    }

    private static void closeQuietly(SeaTunnelClient engineClient) {
        if (engineClient != null) {
            try {
                engineClient.close();
            } catch (Exception e) {
                log.warn("Failed to close the SeaTunnel client", e);
            }
        }
    }

    private static void shutdownQuietly(HazelcastInstanceImpl node) {
        if (node != null) {
            try {
                node.shutdown();
            } catch (Exception e) {
                log.warn("Failed to shut down the embedded cluster node", e);
            }
        }
    }
}

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
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end regression coverage for the client-side checkpoint-restore precondition: a restore
 * request whose source job has no restore-eligible checkpoint data must fail before a job is
 * submitted, never fall back to a fresh execution.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code restoreMode=CHECKPOINT} is
 * allowed to target a historical terminal job, and the documented contract is explicit: "If the
 * checkpoint data for {@code restoreSourceJobId} is missing, already cleaned up, or incompatible,
 * the submission fails fast." The dangerous failure mode is silent: when the source job is canceled
 * with the default {@code checkpoint.retain-after-job-cancelled=false}, its checkpoint data is
 * deleted, so a restore that ignores the empty checkpoint list starts the job from offset zero and
 * every sink that is not idempotent duplicates the already committed rows. The job looks RUNNING
 * and the operator only discovers the data duplication downstream.
 *
 * <p><b>Why the existing coverage does not reach it.</b> The server-side restore entry points are
 * already guarded and unit-tested: {@code RestJobExecutionEnvironment} and {@code
 * ApplicationJobExecutionEnvironment} throw {@code "No checkpoint found for jobId=..."}, and {@code
 * RestApiSubmitJobStartWithSavePointTest} pins the HTTP 400 response. The CLI/client entry point
 * {@code ClientJobExecutionEnvironment} has no such guard, so the same request that REST rejects is
 * silently accepted through the client API. Existing E2E restore tests always restore from a source
 * job whose checkpoint data still exists, so an empty-checkpoint fallback is invisible to them.
 * This test drives the client API only: it produces a real checkpoint, cancels the job with the
 * default retention setting, waits until the storage has been cleaned up, and then asserts that a
 * checkpoint restore into a new job id is rejected instead of being turned into a fresh run.
 *
 * <p>Kept as its own in-JVM cluster class rather than a {@code SeaTunnelEngineContainer} subclass:
 * the contract is about client/server restore bookkeeping and needs no containerized connector, so
 * two embedded Zeta nodes keep the test fast and repeatable.
 */
@Slf4j
public class ClientCheckpointRestoreMissingDataIT {

    private static final String CONF_FILE =
            "stream_fake_to_console_checkpoint_restore_missing.conf";
    private static final String JOB_NAME = "ClientCheckpointRestoreMissingDataIT";
    private static final long STATE_TIMEOUT_SECONDS = 120;

    @Test
    public void testClientCheckpointRestoreFailsWhenSourceCheckpointsWereDeleted()
            throws Exception {
        String testClusterName =
                "ClientCheckpointRestoreMissingDataIT_" + System.currentTimeMillis();
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy silentlyRestoredJob = null;
        try {
            SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            clusterConfig
                    .getHazelcastConfig()
                    .setClusterName(TestUtils.getClusterName(testClusterName));
            // The REST endpoint is irrelevant for the client path and would only contend for the
            // fixed port with other tests.
            clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

            node1 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            awaitClusterSize(node1, 2);

            Common.setDeployMode(DeployMode.CLIENT);
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();
            String confPath = TestUtils.getResource(CONF_FILE);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(JOB_NAME);
            ClientJobProxy sourceJob =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, clusterConfig)
                            .execute();
            long sourceJobId = sourceJob.getJobId();
            awaitJobStatus(sourceJob, JobStatus.RUNNING);

            // The restore precondition can only be exercised once the source job has produced
            // checkpoint data: this proves the checkpoint pipeline is live and that the later
            // empty result comes from the cancellation cleanup rather than from a job that never
            // checkpointed at all.
            JobPipelineCheckpointData checkpoint =
                    awaitCheckpointData(jobClient, sourceJobId, "before cancel");
            Assertions.assertTrue(
                    checkpoint.getCheckpointId() > 0,
                    "Expected a completed checkpoint before cancelling the job");

            // retain-after-job-cancelled defaults to false, so cancellation must delete the
            // checkpoint data of this job.
            engineClient.cancelJob(sourceJobId);
            awaitJobStatus(sourceJob, JobStatus.CANCELED);
            awaitNoCheckpointData(jobClient, sourceJobId);
            log.info(
                    "Checkpoint data of canceled job {} has been cleaned up; a checkpoint restore "
                            + "from it can no longer succeed",
                    sourceJobId);

            long restoreJobId = jobClient.getNewJobId();
            JobConfig restoreJobConfig = new JobConfig();
            restoreJobConfig.setName(JOB_NAME + "_restore");

            Throwable restoreFailure = null;
            try {
                silentlyRestoredJob =
                        engineClient
                                .restoreFromCheckpointExecutionContext(
                                        confPath,
                                        Collections.emptyList(),
                                        restoreJobConfig,
                                        clusterConfig,
                                        sourceJobId,
                                        restoreJobId)
                                .execute();
            } catch (Throwable t) {
                restoreFailure = t;
            }

            if (restoreFailure == null) {
                // The client submitted the restore anyway: this is the silent-fallback regression.
                // Cancel the unexpected job before failing so the embedded cluster can shut down.
                String unexpectedStatus = statusQuietly(silentlyRestoredJob);
                try {
                    engineClient.cancelJob(restoreJobId);
                } catch (Throwable cancelFailure) {
                    log.warn("Could not cancel unexpectedly restored job {}", restoreJobId);
                }
                Assertions.fail(
                        "A checkpoint restore from canceled job "
                                + sourceJobId
                                + " whose checkpoint data was already deleted must be rejected, but"
                                + " the client submitted destination job "
                                + restoreJobId
                                + " (status="
                                + unexpectedStatus
                                + ") and would replay the job from offset zero");
            }

            String message = fullMessage(restoreFailure);
            Assertions.assertTrue(
                    message.contains("No checkpoint found"),
                    "Restore must fail fast with a clear 'No checkpoint found' message, but got: "
                            + message);
            Assertions.assertTrue(
                    message.contains(String.valueOf(sourceJobId)),
                    "Restore failure must identify the source job id "
                            + sourceJobId
                            + ", but got: "
                            + message);

            // A rejected restore must not leave any checkpoint data behind for the destination job.
            Assertions.assertTrue(
                    jobClient.getCheckpointData(restoreJobId, RestoreMode.CHECKPOINT).isEmpty(),
                    "Rejected checkpoint restore must not create checkpoint data for job "
                            + restoreJobId);
        } finally {
            if (silentlyRestoredJob != null) {
                try {
                    silentlyRestoredJob.cancelJob();
                } catch (Throwable ignored) {
                    // The job may already be gone; teardown must not mask the test result.
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

    private static String statusQuietly(ClientJobProxy jobProxy) {
        try {
            return String.valueOf(jobProxy.getJobStatus());
        } catch (Throwable t) {
            return "unknown";
        }
    }

    private static String fullMessage(Throwable throwable) {
        StringBuilder builder = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && !builder.toString().contains(message)) {
                builder.append(message).append(' ');
            }
            current = current.getCause();
        }
        if (builder.length() == 0) {
            builder.append(ExceptionUtils.getMessage(throwable));
        }
        return builder.toString();
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
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

    /** Waits until the job has at least one restore-eligible periodic checkpoint. */
    private static JobPipelineCheckpointData awaitCheckpointData(
            JobClient jobClient, long jobId, String description) {
        AtomicReference<JobPipelineCheckpointData> latest = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT);
                            Assertions.assertFalse(
                                    checkpoints.isEmpty(),
                                    "No checkpoint for "
                                            + description
                                            + " of job "
                                            + jobId
                                            + ": "
                                            + checkpoints);
                            latest.set(checkpoints.get(checkpoints.size() - 1));
                        });
        return latest.get();
    }

    /** Waits until the checkpoint storage holds no restore-eligible data for the job. */
    private static void awaitNoCheckpointData(JobClient jobClient, long jobId) {
        AtomicInteger observed = new AtomicInteger(-1);
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT);
                            observed.set(checkpoints.size());
                            Assertions.assertTrue(
                                    checkpoints.isEmpty(),
                                    "Canceled job "
                                            + jobId
                                            + " must have its checkpoint data deleted when"
                                            + " checkpoint.retain-after-job-cancelled is false, but"
                                            + " "
                                            + observed.get()
                                            + " checkpoint(s) are still stored: "
                                            + checkpoints);
                        });
    }
}

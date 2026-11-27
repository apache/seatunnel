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

package org.apache.seatunnel.engine.e2e.retrybackoff;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end regression coverage for cancelling a job while its pipeline is sleeping in the restore
 * backoff ({@code job.retry.interval.seconds}).
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A failed pipeline does not restore
 * immediately: {@code SubPlan#prepareRestorePipeline()} resets the pipeline and then waits out the
 * configured retry interval. That wait used to be a straight {@code Thread.sleep} executed inside
 * the {@code synchronized} {@code stateProcess()} of the {@code SubPlan}, while {@code
 * cancelPipeline()} is {@code synchronized} on the same object. A cancel issued during the backoff
 * therefore blocked on the pipeline monitor for the whole remaining interval; because the job's
 * {@code needRestore} flag was already cleared before that block, the woken retry path still
 * allocated a fresh set of slots and deployed another attempt of a job the user had already
 * cancelled. The user-visible damage is a cancellation that appears hung for the length of the
 * retry interval, plus slot pressure and real side effects (connections opened, splits read) from a
 * zombie attempt. The engine's only outward signal is the terminal status arriving late, which is
 * exactly what this test pins.
 *
 * <p><b>Why the existing coverage does not reach it.</b> The fault-tolerance ITs cancel jobs in
 * {@code RUNNING} or wait for a restore to complete; the pending-job ITs cancel jobs that never
 * started; {@code RestApiIT} stops jobs without a retry interval in flight. No test cancels a job
 * while a pipeline is inside its retry backoff, so the monitor/wait interaction and the missing
 * post-backoff revocation check are unobserved end to end.
 *
 * <p><b>How the contract is pinned.</b> A batch job whose sink always fails runs with {@code
 * job.retry.times = 3} and a deliberately long {@code job.retry.interval.seconds = 60}. The test
 * waits until attempt 1 has failed and the pipeline is in the backoff (restore counter 1, pipeline
 * status {@code CREATED}), then cancels the job and requires that: the job reaches {@code CANCELED}
 * well before the retry interval could expire; the terminal {@code CANCELED} state is stable
 * afterwards; the pipeline ends {@code CANCELED} too; and the engine never created a second sink
 * writer, i.e. no further attempt was deployed for the cancelled job.
 *
 * <p>Both latencies are measured from the fixture's first failed write ({@link
 * RetryBackoffFailingSinkWriter#firstFailedWriteNanos()}), which the engine necessarily reaches
 * before it can start backing off, never after. That anchor matters: the backoff window is detected
 * by polling, so a timer started when the poll happens to see {@code CREATED} can be started late
 * into the interval. Measured that way the test could observe only a fraction of the remaining
 * backoff and still pass against an engine that blocks the cancel for the whole interval. Anchored
 * to the failure, the assertion is independent of the observing poll: a monitor-held sleep cannot
 * produce the terminal state before the 60s interval expires, while a cancellation that interrupts
 * the wait does so within a few seconds.
 *
 * <p>As a control, the same failing pipeline is then submitted with a short retry interval and a
 * single retry and is *not* cancelled: it must still wait for its interval before deploying the
 * next attempt and must still consume its retry budget. The retry delay is again measured from the
 * control job's own first failed write, not from a later poll, so the control cannot fail a correct
 * engine just because the observation happened late in the interval. Both halves are needed: waking
 * a backoff must not silently turn every retry into an immediate redeploy.
 *
 * <p>Kept as its own class rather than folded into an existing IT because the assertion needs a
 * dedicated fixture with an unusual (one minute) retry interval and a deterministic always-failing
 * sink, and because the in-JVM master/worker cluster here is what makes the pipeline state, restore
 * counter and retry deployment directly observable.
 */
@Slf4j
public class CancelDuringPipelineRetryBackoffIT {

    private static final String CONF_FILE = "batch_retry_backoff_failing_sink.conf";

    /** Same failing pipeline with a {@code job.retry.times = 1} / 3s retry control setup. */
    private static final String SHORT_RETRY_CONF_FILE =
            "batch_retry_backoff_short_interval_failing_sink.conf";

    /** Must match {@code job.retry.interval.seconds} of {@link #CONF_FILE}. */
    private static final long RETRY_INTERVAL_SECONDS = 60L;

    /**
     * How long after the attempt-1 write failure the job may still take to reach {@code CANCELED}.
     * The failure is the earliest instant at which the retry backoff can start, so the deadline
     * does not depend on when the status poll saw the {@code CREATED} window. Half the retry
     * interval leaves a correct cancellation several seconds of head room while the monitor-held
     * sleep cannot finish before the whole 60s interval has expired.
     */
    private static final long CANCEL_DEADLINE_SECONDS = RETRY_INTERVAL_SECONDS / 2;

    /**
     * Upper bound for the control retry, measured from its own first failed write: failure
     * detection plus the 3s backoff plus redeploy must fit inside it. Generous on purpose, its job
     * is to fail the assertion rather than the awaitility timeout when the retry never comes.
     */
    private static final long CONTROL_RETRY_DEADLINE_SECONDS = 60L;

    private static final long CLUSTER_TIMEOUT_SECONDS = 60L;
    private static final long BACKOFF_TIMEOUT_SECONDS = 180L;

    /** Must match {@code job.retry.interval.seconds} of {@link #SHORT_RETRY_CONF_FILE}. */
    private static final long SHORT_RETRY_INTERVAL_SECONDS = 3L;

    @Test
    public void testCancelDuringRestoreBackoffSkipsTheRestore() throws Exception {
        RetryBackoffFailingSinkWriter.reset();
        String testClusterName = "CancelDuringPipelineRetryBackoffIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The HTTP endpoint is irrelevant here and would only contend for fixed ports with other
        // test classes running in the same CI job.
        clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            awaitClusterSize(node1, 2);
            HazelcastInstanceImpl[] nodes = new HazelcastInstanceImpl[] {node1, node2};

            Common.setDeployMode(DeployMode.CLIENT);
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("cancel-during-restore-backoff");
            long jobSubmittedNanos = System.nanoTime();
            ClientJobProxy jobProxy =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(CONF_FILE), jobConfig, clusterConfig)
                            .execute();
            long jobId = jobProxy.getJobId();
            log.warn("Submitted failing job {} for the retry backoff boundary", jobId);

            // Attempt 1 must have failed and the pipeline must be inside the retry backoff. The
            // pipeline is reset to CREATED right before the backoff sleep, so the restore counter
            // and this status together identify the window unambiguously.
            SubPlan pipeline = awaitPipelineInRetryBackoff(nodes, jobId);
            int writersBeforeCancel = RetryBackoffFailingSinkWriter.OPENED.get();
            Assertions.assertTrue(
                    writersBeforeCancel >= 1,
                    "The first attempt must have deployed its sink writer before the backoff"
                            + " starts, but none was created");
            Assertions.assertEquals(
                    PipelineStatus.CREATED,
                    pipeline.getCurrPipelineStatus(),
                    "The pipeline must be reset to CREATED while it waits for its restore");

            // Anchor every latency below to the fixture's first failed write instead of to this
            // observation point: the poll above may see the CREATED window at any moment inside the
            // backoff, while the engine cannot start backing off before the write has failed.
            long firstFailureNanos = RetryBackoffFailingSinkWriter.firstFailedWriteNanos();
            Assertions.assertTrue(
                    firstFailureNanos > jobSubmittedNanos,
                    "The retry backoff must be anchored to this job's own write failure, but the"
                            + " recorded failure ("
                            + firstFailureNanos
                            + ") does not postdate the job submission ("
                            + jobSubmittedNanos
                            + ")");

            long cancelStartedNanos = System.nanoTime();
            long cancelDelayFromFailureMillis =
                    TimeUnit.NANOSECONDS.toMillis(cancelStartedNanos - firstFailureNanos);
            Assertions.assertTrue(
                    cancelDelayFromFailureMillis
                            < TimeUnit.SECONDS.toMillis(RETRY_INTERVAL_SECONDS),
                    "The cancel request must be issued while the "
                            + RETRY_INTERVAL_SECONDS
                            + "s retry backoff is still running, otherwise the test proves"
                            + " nothing; it was issued "
                            + cancelDelayFromFailureMillis
                            + " ms after the write failure that started the backoff");

            jobProxy.cancelJob();
            awaitJobStatus(jobProxy, JobStatus.CANCELED);
            long terminalNanos = System.nanoTime();
            long cancelMillis = TimeUnit.NANOSECONDS.toMillis(terminalNanos - cancelStartedNanos);
            long terminalMillisFromFailure =
                    TimeUnit.NANOSECONDS.toMillis(terminalNanos - firstFailureNanos);
            log.warn(
                    "Job {} reached CANCELED {} ms after the cancel request and {} ms after the"
                            + " write failure that started the {}s retry backoff",
                    jobId,
                    cancelMillis,
                    terminalMillisFromFailure,
                    RETRY_INTERVAL_SECONDS);

            // The bounded deadline is measured from the backoff start anchor, so a late status poll
            // cannot shrink it and let a blocking engine pass.
            Assertions.assertTrue(
                    terminalMillisFromFailure < TimeUnit.SECONDS.toMillis(CANCEL_DEADLINE_SECONDS),
                    "Cancelling a job during its "
                            + RETRY_INTERVAL_SECONDS
                            + "s restore backoff must interrupt the wait instead of waiting for the"
                            + " backoff to expire, but the job reached CANCELED "
                            + terminalMillisFromFailure
                            + " ms after the write failure that started the backoff and "
                            + cancelMillis
                            + " ms after the cancel request");

            // No zombie attempt may be deployed for a job that is already being cancelled. Watch
            // for a short while so a late deployment cannot slip in after the terminal state.
            Awaitility.await()
                    .during(2, TimeUnit.SECONDS)
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            writersBeforeCancel,
                                            RetryBackoffFailingSinkWriter.OPENED.get(),
                                            "The cancelled job must not deploy another attempt"));

            // The terminal state must stay stable and the pipeline must end CANCELED, not FAILED.
            Assertions.assertEquals(JobStatus.CANCELED, jobProxy.getJobStatus());
            Assertions.assertEquals(
                    PipelineStatus.CANCELED,
                    pipeline.getCurrPipelineStatus(),
                    "The cancelled pipeline must end CANCELED instead of being restored");
            Assertions.assertEquals(
                    1,
                    pipeline.getPipelineRestoreNum(),
                    "Cancelling during the backoff must not start a further restore attempt");

            // Control: a retry that is not cancelled must still wait for the configured interval
            // and use up the configured retry budget. This pins that the wake-up above did not
            // turn the backoff into a no-op for everyone else. The fixture's failure clock is
            // restarted so the delay below is anchored to this job's attempt 1, not the cancelled
            // job's.
            RetryBackoffFailingSinkWriter.resetFailureSignal();
            JobConfig controlJobConfig = new JobConfig();
            controlJobConfig.setName("retry-backoff-control");
            long controlSubmittedNanos = System.nanoTime();
            ClientJobProxy controlJob =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(SHORT_RETRY_CONF_FILE),
                                    controlJobConfig,
                                    clusterConfig)
                            .execute();
            long controlJobId = controlJob.getJobId();
            SubPlan controlPipeline = awaitPipelineInRetryBackoff(nodes, controlJobId);
            int writersBeforeRetry = RetryBackoffFailingSinkWriter.OPENED.get();
            long controlFailureNanos = RetryBackoffFailingSinkWriter.firstFailedWriteNanos();
            Assertions.assertTrue(
                    controlFailureNanos > controlSubmittedNanos,
                    "The control retry must be anchored to the control job's own write failure,"
                            + " but the recorded failure ("
                            + controlFailureNanos
                            + ") does not postdate its submission ("
                            + controlSubmittedNanos
                            + ")");
            Awaitility.await()
                    .atMost(BACKOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            RetryBackoffFailingSinkWriter.OPENED.get()
                                                    > writersBeforeRetry,
                                            "The uncancelled job must deploy its second attempt"));
            long retryDelayMillis =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - controlFailureNanos);
            log.warn(
                    "Control job {} deployed its second attempt {} ms after its attempt-1 write"
                            + " failure with a {}s retry interval",
                    controlJobId,
                    retryDelayMillis,
                    SHORT_RETRY_INTERVAL_SECONDS);
            // Lower bound: the second attempt cannot start before the configured interval has
            // elapsed since the failure that authorized the retry. Unlike a timer started at the
            // CREATED observation, this cannot shrink when the poll is late.
            Assertions.assertTrue(
                    retryDelayMillis >= TimeUnit.SECONDS.toMillis(SHORT_RETRY_INTERVAL_SECONDS),
                    "An uncancelled retry must still wait for its "
                            + SHORT_RETRY_INTERVAL_SECONDS
                            + "s backoff, but the next attempt started "
                            + retryDelayMillis
                            + " ms after the attempt-1 write failure");
            Assertions.assertTrue(
                    retryDelayMillis < TimeUnit.SECONDS.toMillis(CONTROL_RETRY_DEADLINE_SECONDS),
                    "The uncancelled retry must deploy its second attempt within "
                            + CONTROL_RETRY_DEADLINE_SECONDS
                            + "s of the attempt-1 write failure, but it took "
                            + retryDelayMillis
                            + " ms");
            awaitJobStatus(controlJob, JobStatus.FAILED);
            Assertions.assertEquals(
                    1,
                    controlPipeline.getPipelineRestoreNum(),
                    "The control job must consume its single configured retry");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node2 != null) {
                node2.shutdown();
            }
            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    private static SubPlan awaitPipelineInRetryBackoff(HazelcastInstanceImpl[] nodes, long jobId) {
        AtomicReference<SubPlan> backoffPipeline = new AtomicReference<>();
        Awaitility.await()
                .atMost(BACKOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SubPlan pipeline = findPipeline(nodes, jobId);
                            Assertions.assertNotNull(
                                    pipeline, "Job " + jobId + " has no observable pipeline yet");
                            Assertions.assertEquals(
                                    1,
                                    pipeline.getPipelineRestoreNum(),
                                    "Waiting for attempt 1 of job "
                                            + jobId
                                            + " to fail and enter its retry backoff, current status "
                                            + pipeline.getCurrPipelineStatus());
                            Assertions.assertEquals(
                                    PipelineStatus.CREATED,
                                    pipeline.getCurrPipelineStatus(),
                                    "Waiting for job " + jobId + " to sleep in its retry backoff");
                            backoffPipeline.set(pipeline);
                        });
        return backoffPipeline.get();
    }

    private static SubPlan findPipeline(HazelcastInstanceImpl[] nodes, long jobId) {
        for (HazelcastInstanceImpl node : nodes) {
            JobMaster jobMaster = findJobMaster(node, jobId);
            if (jobMaster == null) {
                continue;
            }
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            if (!pipelines.isEmpty()) {
                return pipelines.get(0);
            }
        }
        return null;
    }

    private static JobMaster findJobMaster(HazelcastInstanceImpl node, long jobId) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        if (server == null) {
            return null;
        }
        CoordinatorService coordinatorService = server.getCoordinatorService();
        if (coordinatorService == null) {
            return null;
        }
        return coordinatorService.getJobMaster(jobId);
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(CANCEL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Waiting for job "
                                                + jobProxy.getJobId()
                                                + " to reach "
                                                + expectedStatus));
    }
}

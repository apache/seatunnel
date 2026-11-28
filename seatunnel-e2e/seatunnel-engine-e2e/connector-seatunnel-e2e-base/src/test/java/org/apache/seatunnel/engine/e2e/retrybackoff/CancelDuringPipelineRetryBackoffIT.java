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
 * job.retry.times = 3} and a deliberately long {@code job.retry.interval.seconds = 60}. The fixture
 * gates the attempt-1 write, so the failure only happens after the test has announced itself and is
 * already waiting for it ({@link RetryBackoffFailingSinkWriter#armFirstWriteGate()}); the test then
 * releases the gate and waits for the engine's durable record that the restore was authorized
 * ({@code pipelineRestoreNum == 1}), which the engine writes immediately before it waits out the
 * interval. Only then does the test cancel the job. Nothing in that path depends on catching a
 * transient pipeline state: the restore counter only grows, and the attempt timestamps and writer
 * counter are recorded by the fixture on the engine threads.
 *
 * <p>The test then requires that: the job reaches {@code CANCELED} within {@link
 * #CANCEL_LATENCY_DEADLINE_SECONDS} of the cancel request, and before the 60s backoff could have
 * expired; the terminal {@code CANCELED} state is stable afterwards; the pipeline ends {@code
 * CANCELED} too; and the engine never created a second sink writer, i.e. no further attempt was
 * deployed for the cancelled job. The cancel latency is measured from the cancel request, so
 * scheduling delay before that request can no longer eat into the budget the assertion gives the
 * engine, while the "it did not just wait out the backoff" check is anchored to the fixture's first
 * failed write, which the engine necessarily reaches before it can start backing off.
 *
 * <p><b>Why this is deterministic and not just slow.</b> The budget is spent only after the cancel
 * is issued (the latency from the cancel to the terminal state), and the fixture's controlled
 * release makes the failure that starts the backoff happen while the test is already waiting for
 * it. The 60s interval is orders of magnitude larger than the sub-second path from that release to
 * the cancel request, so no plausible scheduling jitter can push the request outside the interval,
 * and every measurement reads a timestamp recorded by the fixture rather than the instant a poll
 * happened to run.
 *
 * <p>As a control, the same failing pipeline is then submitted with a short retry interval and a
 * single retry and is <em>not</em> cancelled: it must still wait for its interval before deploying
 * the next attempt and must still consume its retry budget. That delay is the difference between
 * the fixture's attempt-1 failure timestamp and its attempt-2 writer-open timestamp, so a late poll
 * can neither shrink nor inflate it, and "the second attempt happened" is decided by the fixture's
 * recorded attempt counter and timestamps rather than by observing {@code CREATED}. Both halves are
 * needed: waking a backoff must not silently turn every retry into an immediate redeploy.
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

    /** Must match {@code job.retry.interval.seconds} of {@link #SHORT_RETRY_CONF_FILE}. */
    private static final long SHORT_RETRY_INTERVAL_SECONDS = 3L;

    /**
     * How long the cancelled job may take from the cancel request to {@code CANCELED}.
     *
     * <p>This is the budget that is actually spent after the cancel is issued, so a delayed test
     * thread cannot consume it before the action is taken. It is generous for a cancellation that
     * interrupts the wait (a handful of milliseconds in practice) and far below the remaining
     * backoff an engine that blocks the cancel would still have to sit out.
     */
    private static final long CANCEL_LATENCY_DEADLINE_SECONDS = 10L;

    /**
     * Generous bound on waiting for the cancelled job's terminal state. It exceeds the whole retry
     * interval on purpose: an engine that blocks the cancel still reaches {@code CANCELED} when the
     * interval expires, and the latency assertion above should then report the measured delay
     * instead of an awaitility timeout.
     */
    private static final long CANCELED_TIMEOUT_SECONDS = RETRY_INTERVAL_SECONDS + 30L;

    /**
     * How long the engine may take to record that attempt 1 of the job was authorized for a
     * restore. This is a durable counter, not a transient state, so the bound only guards against a
     * job that never fails at all.
     */
    private static final long RESTORE_AUTHORIZED_TIMEOUT_SECONDS = 60L;

    /**
     * Upper bound for the control retry, measured from its own first failed write: failure
     * detection plus the 3s backoff plus redeploy must fit inside it. Generous on purpose, its job
     * is to fail the assertion rather than the awaitility timeout when the retry never comes.
     */
    private static final long CONTROL_RETRY_DEADLINE_SECONDS = 60L;

    private static final long CLUSTER_TIMEOUT_SECONDS = 60L;

    /** How long the gated first write may wait for the test to be ready before it fails anyway. */
    private static final long FIRST_WRITE_GATE_TIMEOUT_SECONDS = 60L;

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

            // The fixture must block the attempt-1 write until this thread releases it, so the
            // write failure - and therefore the entry into the retry backoff - happens only once
            // the test is already waiting for it instead of racing a later poll.
            RetryBackoffFailingSinkWriter.armFirstWriteGate();

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

            try {
                // Controlled release: the first attempt is already at its failing write, so the
                // failure below and the cancel that follows it are not separated by an arbitrary
                // amount of observation latency.
                Assertions.assertTrue(
                        RetryBackoffFailingSinkWriter.awaitFirstWriteReached(
                                FIRST_WRITE_GATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                        "The first attempt must reach its failing write within "
                                + FIRST_WRITE_GATE_TIMEOUT_SECONDS
                                + "s");
                RetryBackoffFailingSinkWriter.releaseFirstWrite();

                // Durable evidence that attempt 1 failed and the engine authorized the restore:
                // the counter is incremented right before the backoff and never decreases. No
                // transient pipeline status (such as CREATED during the wait) is required, so a
                // delayed observer cannot miss the window and time out.
                SubPlan pipeline = awaitRestoreAuthorized(nodes, jobId);
                long firstFailureNanos =
                        RetryBackoffFailingSinkWriter.attemptFirstFailedWriteNanos(1);
                Assertions.assertTrue(
                        firstFailureNanos > jobSubmittedNanos,
                        "The retry backoff must be anchored to this job's own write failure, but"
                                + " the recorded failure ("
                                + firstFailureNanos
                                + ") does not postdate the job submission ("
                                + jobSubmittedNanos
                                + ")");
                Assertions.assertEquals(
                        1,
                        pipeline.getPipelineRestoreNum(),
                        "The cancel must be issued after the first restore was authorized and"
                                + " before any further one");
                int writersBeforeCancel = RetryBackoffFailingSinkWriter.OPENED.get();
                Assertions.assertEquals(
                        1,
                        writersBeforeCancel,
                        "The first attempt must have deployed its sink writer before the backoff"
                                + " starts, and no further attempt may start inside the "
                                + RETRY_INTERVAL_SECONDS
                                + "s interval");

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
                awaitJobStatus(jobProxy, JobStatus.CANCELED, CANCELED_TIMEOUT_SECONDS);
                long terminalNanos = System.nanoTime();
                long cancelMillis =
                        TimeUnit.NANOSECONDS.toMillis(terminalNanos - cancelStartedNanos);
                long terminalMillisFromFailure =
                        TimeUnit.NANOSECONDS.toMillis(terminalNanos - firstFailureNanos);
                log.warn(
                        "Job {} reached CANCELED {} ms after the cancel request and {} ms after the"
                                + " write failure that started the {}s retry backoff",
                        jobId,
                        cancelMillis,
                        terminalMillisFromFailure,
                        RETRY_INTERVAL_SECONDS);

                // The cancel itself must not have waited on the pipeline monitor. The budget is
                // measured from the cancel request, so observation latency before the request
                // cannot make a correct engine fail this assertion.
                Assertions.assertTrue(
                        cancelMillis < TimeUnit.SECONDS.toMillis(CANCEL_LATENCY_DEADLINE_SECONDS),
                        "Cancelling a job during its "
                                + RETRY_INTERVAL_SECONDS
                                + "s restore backoff must interrupt the wait instead of blocking"
                                + " on the pipeline monitor, but the job reached CANCELED "
                                + cancelMillis
                                + " ms after the cancel request");

                // And the terminal state must have arrived before the backoff could expire, which
                // is the direct negation of the regression: a blocked cancel finishes only after
                // the whole interval.
                Assertions.assertTrue(
                        terminalMillisFromFailure
                                < TimeUnit.SECONDS.toMillis(RETRY_INTERVAL_SECONDS),
                        "The cancelled job reached CANCELED "
                                + terminalMillisFromFailure
                                + " ms after the write failure that started the "
                                + RETRY_INTERVAL_SECONDS
                                + "s backoff, i.e. it waited for the backoff to expire");

                // No zombie attempt may be deployed for a job that is already being cancelled.
                // Keep watching for a short while so a late deployment cannot slip in after the
                // terminal state.
                Awaitility.await()
                        .during(2, TimeUnit.SECONDS)
                        .atMost(5, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                writersBeforeCancel,
                                                RetryBackoffFailingSinkWriter.OPENED.get(),
                                                "The cancelled job must not deploy another attempt"));

                // The terminal state must stay stable and the pipeline must end CANCELED, not
                // FAILED.
                Assertions.assertEquals(JobStatus.CANCELED, jobProxy.getJobStatus());
                Assertions.assertEquals(
                        PipelineStatus.CANCELED,
                        pipeline.getCurrPipelineStatus(),
                        "The cancelled pipeline must end CANCELED instead of being restored");
                Assertions.assertEquals(
                        1,
                        pipeline.getPipelineRestoreNum(),
                        "Cancelling during the backoff must not start a further restore attempt");
            } finally {
                // Never leave an engine task parked on the gate, including on the failure paths.
                RetryBackoffFailingSinkWriter.releaseFirstWrite();
            }

            // Control: a retry that is not cancelled must still wait for the configured interval
            // and use up the configured retry budget. This pins that the wake-up above did not
            // turn the backoff into a no-op for everyone else. The fixture is fully reset, so the
            // attempt numbering and timestamps below belong to the control job's own attempts.
            RetryBackoffFailingSinkWriter.reset();
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
            SubPlan controlPipeline = awaitRestoreAuthorized(nodes, controlJobId);
            long controlFailureNanos =
                    RetryBackoffFailingSinkWriter.attemptFirstFailedWriteNanos(1);
            Assertions.assertTrue(
                    controlFailureNanos > controlSubmittedNanos,
                    "The control retry must be anchored to the control job's own write failure,"
                            + " but the recorded failure ("
                            + controlFailureNanos
                            + ") does not postdate its submission ("
                            + controlSubmittedNanos
                            + ")");

            // "The second attempt was deployed" is a durable fixture fact: the writer counter and
            // the attempt-2 open timestamp are recorded by the engine as it deploys the attempt,
            // so waiting for them cannot shift the measured delay.
            Awaitility.await()
                    .atMost(CONTROL_RETRY_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            RetryBackoffFailingSinkWriter.attemptOpenNanos(2) > 0L,
                                            "The uncancelled job must deploy its second attempt"));
            long retryDelayMillis =
                    TimeUnit.NANOSECONDS.toMillis(
                            RetryBackoffFailingSinkWriter.attemptOpenNanos(2)
                                    - controlFailureNanos);
            log.warn(
                    "Control job {} deployed its second attempt {} ms after its attempt-1 write"
                            + " failure with a {}s retry interval",
                    controlJobId,
                    retryDelayMillis,
                    SHORT_RETRY_INTERVAL_SECONDS);
            // Lower bound: the second attempt cannot open before the configured interval has
            // elapsed since the failure that authorized the retry. Both ends of the interval are
            // fixture timestamps, so a late poll cannot shrink the measurement.
            Assertions.assertTrue(
                    retryDelayMillis >= TimeUnit.SECONDS.toMillis(SHORT_RETRY_INTERVAL_SECONDS),
                    "An uncancelled retry must still wait for its "
                            + SHORT_RETRY_INTERVAL_SECONDS
                            + "s backoff, but the next attempt opened "
                            + retryDelayMillis
                            + " ms after the attempt-1 write failure");
            Assertions.assertTrue(
                    retryDelayMillis < TimeUnit.SECONDS.toMillis(CONTROL_RETRY_DEADLINE_SECONDS),
                    "The uncancelled retry must deploy its second attempt within "
                            + CONTROL_RETRY_DEADLINE_SECONDS
                            + "s of the attempt-1 write failure, but it took "
                            + retryDelayMillis
                            + " ms");
            awaitJobStatus(controlJob, JobStatus.FAILED, CONTROL_RETRY_DEADLINE_SECONDS);
            Assertions.assertEquals(
                    1,
                    controlPipeline.getPipelineRestoreNum(),
                    "The control job must consume its single configured retry");
            Assertions.assertTrue(
                    RetryBackoffFailingSinkWriter.OPENED.get() >= 2,
                    "The control job must have opened the writers of both of its attempts, but"
                            + " OPENED is "
                            + RetryBackoffFailingSinkWriter.OPENED.get());
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

    /**
     * Waits for the engine's durable record that attempt 1 of the job failed and a restore was
     * authorized for it.
     *
     * <p>{@code pipelineRestoreNum} is incremented immediately before the pipeline waits out its
     * retry interval and only ever grows, so this wait cannot miss a short window: it stays true no
     * matter how late the caller looks. A pipeline that already moved on to a further attempt fails
     * the caller's follow-up assertion instead of timing out here.
     */
    private static SubPlan awaitRestoreAuthorized(HazelcastInstanceImpl[] nodes, long jobId) {
        AtomicReference<SubPlan> restoreAuthorized = new AtomicReference<>();
        Awaitility.await()
                .atMost(RESTORE_AUTHORIZED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SubPlan pipeline = findPipeline(nodes, jobId);
                            Assertions.assertNotNull(
                                    pipeline, "Job " + jobId + " has no observable pipeline yet");
                            Assertions.assertTrue(
                                    pipeline.getPipelineRestoreNum() >= 1,
                                    "Waiting for attempt 1 of job "
                                            + jobId
                                            + " to fail and have its restore authorized, current"
                                            + " restore counter "
                                            + pipeline.getPipelineRestoreNum()
                                            + " and status "
                                            + pipeline.getCurrPipelineStatus());
                            restoreAuthorized.set(pipeline);
                        });
        return restoreAuthorized.get();
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

    private static void awaitJobStatus(
            ClientJobProxy jobProxy, JobStatus expectedStatus, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
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

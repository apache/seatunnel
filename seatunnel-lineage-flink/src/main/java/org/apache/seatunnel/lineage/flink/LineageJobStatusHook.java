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

package org.apache.seatunnel.lineage.flink;

import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageRuntime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobStatusHook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Emits the terminal lineage event for a Flink job from the JobManager.
 *
 * <p>This class is the reason the lineage artifact has to be installed in the Flink cluster's
 * {@code lib/} directory. The hook is a structural field of the JobGraph, so the JobManager
 * deserializes it with the system class loader before any user class loader exists; the class is
 * not resolvable from the submitted job jar.
 *
 * <p>The instance is serialized into the JobGraph, which is written to the BlobServer and to HA
 * storage. It therefore carries no credential: the caller strips the token, and the token is
 * resolved again on the JobManager from its own cluster configuration and process environment.
 */
public final class LineageJobStatusHook implements JobStatusHook {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(LineageJobStatusHook.class);

    /** Run-facet property carrying the Flink job identifier shown in the Flink UI and REST API. */
    public static final String FLINK_JOB_ID_PROPERTY = "flink_job_id";

    private final LineageConfig config;
    private final List<LineageEvent> events;
    private final boolean clientReportsCompletion;

    /**
     * Cancelled by whichever terminal callback fires. Transient because the hook is serialized into
     * the JobGraph on the client, where no heartbeat is running; the JobManager starts its own on
     * {@code onCreated}. Volatile because the callbacks that start and stop it are not guaranteed
     * to run on one thread.
     */
    private transient volatile ScheduledFuture<?> heartbeat;

    /**
     * Set by the terminal callback before its event is submitted, so a heartbeat that is already
     * queued cannot report RUNNING after it. Written from the job's state-transition thread and
     * read from the emitting thread, hence volatile. Transient for the same reason as the
     * heartbeat: nothing is terminal on the client that serializes this into the JobGraph.
     */
    private transient volatile boolean terminal;

    /**
     * @param config lineage configuration with the auth token already removed
     * @param events the start events whose runs this hook closes
     * @param clientReportsCompletion whether the submitting client reports the successful terminal
     *     event itself, in which case this hook must not send a second one
     */
    public LineageJobStatusHook(
            LineageConfig config, List<LineageEvent> events, boolean clientReportsCompletion) {
        this.config = config;
        this.events = new ArrayList<>(events);
        this.clientReportsCompletion = clientReportsCompletion;
    }

    @Override
    public void onCreated(JobID jobId) {
        guarded(() -> startHeartbeat(jobId(jobId)));
    }

    @Override
    public void onFinished(JobID jobId) {
        guarded(
                () -> {
                    stopHeartbeat();
                    // An attached client emits the successful terminal event with the output
                    // statistics read from the job accumulators, which are not reachable from here.
                    // Both events describe the same run, and the later arrival wins on the
                    // receiver, so exactly one of the two may send it.
                    if (!clientReportsCompletion) {
                        submitTerminal(LineageEventType.COMPLETE, jobId(jobId));
                    }
                });
    }

    @Override
    public void onFailed(JobID jobId, Throwable cause) {
        guarded(
                () -> {
                    stopHeartbeat();
                    submitTerminal(LineageEventType.FAIL, jobId(jobId));
                });
    }

    @Override
    public void onCanceled(JobID jobId) {
        guarded(
                () -> {
                    stopHeartbeat();
                    submitTerminal(LineageEventType.ABORT, jobId(jobId));
                });
    }

    /**
     * Runs one callback body, absorbing anything it throws.
     *
     * <p>Flink invokes these hooks inline from the job's state transition, so an exception escaping
     * here would fail a job for a lineage problem. Reporting is best effort by contract.
     */
    private static void guarded(Runnable callback) {
        try {
            callback.run();
        } catch (Throwable error) {
            LOGGER.warn("Failed to handle Flink lineage status callback", error);
        }
    }

    /**
     * Renders the Flink job identifier passed to every {@code JobStatusHook} callback.
     *
     * <p>The run ID is derived before submission, when no Flink job ID exists yet, so this is the
     * only point where the lineage run can be tied back to the job visible in the Flink UI.
     */
    private static String jobId(JobID jobId) {
        return jobId == null ? null : jobId.toString();
    }

    /**
     * Starts the periodic RUNNING event that keeps the receiver from treating a still-running job
     * as abandoned.
     *
     * <p>A receiver decides a run was abandoned from how long it has been silent, so a job that
     * outlives that window without reporting has its lineage dropped from the current graph even
     * though it is healthy. Zeta reports from its checkpoint completion callback, but Flink offers
     * the JobManager no periodic callback to hang this on: {@code JobStatusHook} is purely
     * transitional, and the client's {@code JobListener} is gone once a detached submission
     * returns. A scheduled task is therefore the only place this can live for a detached job, which
     * is how streaming jobs are normally submitted.
     *
     * <p>Batch jobs are not excluded. The interval is coarse enough that a batch job finishes long
     * before the first heartbeat, and one that does run for hours needs the heartbeat for the same
     * reason a streaming job does.
     *
     * <p>The first heartbeat is one full interval away because the start event has just refreshed
     * the run.
     */
    private void startHeartbeat(String flinkJobId) {
        long intervalMs = config.heartbeatMinIntervalMs();
        if (intervalMs <= 0) {
            return;
        }
        try {
            heartbeat =
                    Heartbeats.SCHEDULER.scheduleWithFixedDelay(
                            () -> emit(LineageEventType.RUNNING, flinkJobId),
                            intervalMs,
                            intervalMs,
                            TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            // A job must not fail because its lineage heartbeat could not be scheduled.
            LOGGER.warn("Failed to schedule Flink lineage heartbeat", error);
        }
    }

    /**
     * Stops the heartbeat before the terminal event is queued, so no RUNNING can follow it.
     *
     * <p>Both steps are non-blocking, which is the point: Flink runs this on the job's own state
     * transition, and waiting here for an in-flight send would hold that thread for a full retry
     * cycle per output dataset. Cancelling keeps further heartbeats from being scheduled;
     * publishing the terminal flag makes a heartbeat that is already queued, or already inside its
     * loop over the runs, give up rather than report RUNNING after the run has closed.
     */
    private void stopHeartbeat() {
        ScheduledFuture<?> scheduled = heartbeat;
        if (scheduled != null) {
            scheduled.cancel(false);
            heartbeat = null;
        }
        terminal = true;
    }

    /**
     * Queues the terminal event instead of sending it on the job's state-transition thread.
     *
     * <p>A send blocks for the configured timeout on every retry, once per output dataset, so
     * sending here would hold a JobManager thread for minutes against an unreachable receiver and
     * make a lineage failure visible as a stalled job. Queueing also keeps the terminal event
     * behind any heartbeat that is already sending, because the emitting thread runs one task at a
     * time; see {@link Heartbeats}.
     */
    private void submitTerminal(LineageEventType eventType, String flinkJobId) {
        try {
            Heartbeats.SCHEDULER.execute(() -> emit(eventType, flinkJobId));
        } catch (Throwable error) {
            LOGGER.warn("Failed to submit the terminal Flink lineage event", error);
        }
    }

    /**
     * Holds the one scheduler shared by every job on this JobManager.
     *
     * <p>Every lineage send of this JobManager runs here, and a single daemon thread is what orders
     * them: a terminal event queued while a heartbeat is still sending cannot overtake it, so the
     * receiver never sees RUNNING after a run has closed. One thread rather than one per job also
     * bounds what a dead receiver can cost the process, and {@code scheduleWithFixedDelay} lets a
     * slow send push its own next run back instead of piling up. Daemon so a JobManager shutdown is
     * never held open by it.
     */
    private static final class Heartbeats {
        /**
         * How long a shutting-down JobManager waits for queued terminal events.
         *
         * <p>Application-mode clusters shut down as soon as the job reaches a terminal state, which
         * is the same moment the terminal event is queued. Without a wait the run would stay open
         * on the receiver until its abandoned-run timeout. The wait is short because a reachable
         * receiver answers in milliseconds and an unreachable one must not delay the shutdown.
         */
        private static final long DRAIN_TIMEOUT_MS = 5000;

        static final ScheduledExecutorService SCHEDULER =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-lineage-heartbeat");
                            thread.setDaemon(true);
                            return thread;
                        });

        static {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(Heartbeats::drain, "seatunnel-lineage-drain"));
        }

        /**
         * Gives already queued terminal events a bounded chance to leave before the JVM exits.
         *
         * <p>{@code shutdown} drops the periodic heartbeats and keeps the queued terminal events,
         * which is exactly the wanted split.
         */
        private static void drain() {
            SCHEDULER.shutdown();
            try {
                if (!SCHEDULER.awaitTermination(DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    SCHEDULER.shutdownNow();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                SCHEDULER.shutdownNow();
            }
        }

        private Heartbeats() {}
    }

    /**
     * Waits until every event queued so far has been sent.
     *
     * <p>A test seam. The emitting thread runs one task at a time, so a task queued after the
     * events under test completes only once they have.
     *
     * @return whether the queue drained within the timeout
     */
    static boolean awaitPendingEmissions(long timeoutMs) throws InterruptedException {
        try {
            Future<?> barrier = Heartbeats.SCHEDULER.submit(() -> {});
            barrier.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException interrupted) {
            throw interrupted;
        } catch (Exception error) {
            return false;
        }
    }

    /**
     * Sends one lifecycle event for every run this hook owns.
     *
     * <p>Runs only on the shared emitting thread, which orders every send of this JobManager
     * against every other; see {@link Heartbeats}.
     */
    private void emit(LineageEventType eventType, String flinkJobId) {
        try {
            LineageConfig runtimeConfig =
                    config.withAuthToken(
                            LineageConfig.resolveToken(
                                    FlinkClusterOptions.load(), System.getenv()));
            for (LineageEvent event : events) {
                if (eventType == LineageEventType.RUNNING && terminal) {
                    // The terminal event owns this run from here on. Abandoning the rest of the
                    // loop is safe: whatever the terminal callback sends covers every run this
                    // hook owns.
                    return;
                }
                LineageRuntime.emit(
                        runtimeConfig,
                        event.withEventType(eventType)
                                .withRunProperty(FLINK_JOB_ID_PROPERTY, flinkJobId));
            }
        } catch (Throwable error) {
            LOGGER.warn("Failed to emit Flink lineage status event", error);
        }
    }
}

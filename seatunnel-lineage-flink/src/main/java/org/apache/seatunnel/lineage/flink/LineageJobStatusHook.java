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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 *
 * <p>Each hook owns one single-thread scheduler. Heartbeats and the terminal event of the same run
 * stay ordered on that thread, and a slow send for one job cannot hold the terminal event of
 * another job on the same JobManager. The hook always emits {@code COMPLETE}; the client does not,
 * so a lost submitting process still leaves a terminal event.
 */
public final class LineageJobStatusHook implements JobStatusHook {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(LineageJobStatusHook.class);

    /** Run-facet property carrying the Flink job identifier shown in the Flink UI and REST API. */
    public static final String FLINK_JOB_ID_PROPERTY = "flink_job_id";

    private final LineageConfig config;
    private final List<LineageEvent> events;

    /**
     * Cancelled by whichever terminal callback fires. Transient because the hook is serialized into
     * the JobGraph on the client, where no heartbeat is running; the JobManager starts its own on
     * {@code onCreated}. Volatile because the callbacks that start and stop it are not guaranteed
     * to run on one thread.
     */
    private transient volatile ScheduledFuture<?> heartbeat;

    /**
     * Sends this job's heartbeats and terminal event, in that order. Transient because the client
     * that serializes the hook has nothing to schedule; the JobManager creates it on the first
     * callback. One thread per hook keeps a slow job off every other job's lane.
     */
    private transient volatile ScheduledExecutorService scheduler;

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
     */
    public LineageJobStatusHook(LineageConfig config, List<LineageEvent> events) {
        this.config = config;
        this.events = new ArrayList<>(events);
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
                    // The hook owns completion for attached and detached jobs. The client does not
                    // send COMPLETE, so a killed client or a dropped REST connection still closes
                    // the run when the job finishes.
                    submitTerminal(LineageEventType.COMPLETE, jobId(jobId));
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
                    scheduler()
                            .scheduleWithFixedDelay(
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
     * cycle. Cancelling keeps further heartbeats from being scheduled; publishing the terminal flag
     * makes a heartbeat that is already queued, or already inside its loop over the runs, give up
     * rather than report RUNNING after the run has closed.
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
     * <p>A send blocks for the configured timeout on every retry, so sending here would hold a
     * JobManager thread against an unreachable receiver and make a lineage failure visible as a
     * stalled job. Queueing keeps the terminal event behind any heartbeat that is already sending
     * on this job's thread. Another job has its own thread, so it does not wait behind this one.
     */
    private void submitTerminal(LineageEventType eventType, String flinkJobId) {
        try {
            Future<?> task =
                    scheduler()
                            .submit(
                                    () -> {
                                        try {
                                            emit(eventType, flinkJobId);
                                        } finally {
                                            ScheduledExecutorService current = scheduler;
                                            if (current != null) {
                                                current.shutdown();
                                                JobSenders.unregister(current);
                                            }
                                        }
                                    });
            JobSenders.PENDING.add(task);
        } catch (Throwable error) {
            LOGGER.warn("Failed to submit the terminal Flink lineage event", error);
        }
    }

    /**
     * Returns this job's single-thread scheduler, creating it on the first callback.
     *
     * <p>The same thread runs this job's heartbeat and its terminal event, so a RUNNING that is
     * already sending cannot be overtaken. A different job gets a different thread.
     */
    private ScheduledExecutorService scheduler() {
        ScheduledExecutorService current = scheduler;
        if (current != null && !current.isShutdown()) {
            return current;
        }
        synchronized (this) {
            if (scheduler == null || scheduler.isShutdown()) {
                scheduler = JobSenders.create(this);
            }
            return scheduler;
        }
    }

    /**
     * Tracks one scheduler per job and drains them independently on shutdown.
     *
     * <p>A single shared thread let one job's timeout and retries stall every other job's heartbeat
     * and terminal event. Each job now has its own daemon thread. Shutdown gives every job its own
     * drain budget, so one blocked send cannot use up the time the others have to leave.
     */
    private static final class JobSenders {
        /**
         * How long a shutting-down JobManager waits for one job's queued terminal event.
         *
         * <p>Application-mode clusters shut down as soon as the job reaches a terminal state, which
         * is the same moment the terminal event is queued. The wait is per job and runs in parallel
         * across jobs. A reachable receiver answers in milliseconds; an unreachable one must not
         * delay shutdown, and must not cause another job's event to miss its own window.
         */
        private static final long DRAIN_TIMEOUT_MS = 5000;

        private static final Set<ScheduledExecutorService> EXECUTORS =
                ConcurrentHashMap.newKeySet();

        /** Terminal tasks still in flight. Waiting on these publishes the events they recorded. */
        private static final Set<Future<?>> PENDING = ConcurrentHashMap.newKeySet();

        static {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(JobSenders::drain, "seatunnel-lineage-drain"));
        }

        static ScheduledExecutorService create(Object owner) {
            ScheduledExecutorService executor =
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(
                                                runnable,
                                                "seatunnel-lineage-"
                                                        + Integer.toHexString(
                                                                System.identityHashCode(owner)));
                                thread.setDaemon(true);
                                return thread;
                            });
            EXECUTORS.add(executor);
            return executor;
        }

        static void unregister(ScheduledExecutorService executor) {
            EXECUTORS.remove(executor);
        }

        static List<ScheduledExecutorService> snapshot() {
            return new ArrayList<>(EXECUTORS);
        }

        /**
         * Gives already queued terminal events a bounded chance to leave before the JVM exits.
         *
         * <p>{@code shutdown} drops periodic heartbeats and keeps queued terminal events. Each
         * scheduler waits for its own budget, concurrently with the others.
         */
        private static void drain() {
            List<ScheduledExecutorService> executors = snapshot();
            List<Thread> waiters = new ArrayList<>();
            for (ScheduledExecutorService executor : executors) {
                executor.shutdown();
                Thread waiter =
                        new Thread(
                                () -> {
                                    try {
                                        if (!executor.awaitTermination(
                                                DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                                            executor.shutdownNow();
                                        }
                                    } catch (InterruptedException interrupted) {
                                        Thread.currentThread().interrupt();
                                        executor.shutdownNow();
                                    }
                                },
                                "seatunnel-lineage-drain-job");
                waiter.setDaemon(true);
                waiter.start();
                waiters.add(waiter);
            }
            for (Thread waiter : waiters) {
                try {
                    waiter.join(DRAIN_TIMEOUT_MS + 1000L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        private JobSenders() {}
    }

    /**
     * Waits until every event queued so far on every job scheduler has been sent.
     *
     * <p>A test seam. Each job runs one task at a time, so a barrier queued after the events under
     * test completes only once they have. An executor that already shut down after its terminal
     * event is waited on with {@code awaitTermination}, which still happens-before the events it
     * recorded.
     *
     * @param timeoutMs budget for the whole set of schedulers
     * @return whether every scheduler drained within the timeout
     */
    public static boolean awaitPendingEmissions(long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        for (Future<?> pending : new ArrayList<>(JobSenders.PENDING)) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            try {
                pending.get(remaining, TimeUnit.NANOSECONDS);
                JobSenders.PENDING.remove(pending);
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (ExecutionException | TimeoutException error) {
                return false;
            }
        }
        for (ScheduledExecutorService executor : JobSenders.snapshot()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            try {
                if (executor.isShutdown()) {
                    if (!executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                        return false;
                    }
                    continue;
                }
                Future<?> barrier = executor.submit(() -> {});
                barrier.get(remaining, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException rejected) {
                remaining = deadline - System.nanoTime();
                if (remaining <= 0 || !executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                    return false;
                }
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (ExecutionException | TimeoutException error) {
                return false;
            }
        }
        return true;
    }

    /**
     * Sends one lifecycle event for every run this hook owns.
     *
     * <p>Runs only on this job's emitting thread, which orders the heartbeat against the terminal
     * event. The event-set budget, and a failed delivery, both stop the loop before the remaining
     * outputs are attempted.
     */
    private void emit(LineageEventType eventType, String flinkJobId) {
        try {
            LineageConfig runtimeConfig =
                    LineageConfig.resolve(
                            config.toNonSensitiveMap(),
                            FlinkClusterOptions.load(),
                            System.getenv());
            List<LineageEvent> prepared = new ArrayList<>(events.size());
            for (LineageEvent event : events) {
                prepared.add(
                        event.withEventType(eventType)
                                .withRunProperty(FLINK_JOB_ID_PROPERTY, flinkJobId));
            }
            LineageRuntime.emitEach(
                    runtimeConfig,
                    prepared,
                    () -> eventType == LineageEventType.RUNNING && terminal);
        } catch (Throwable error) {
            LOGGER.warn("Failed to emit Flink lineage status event", error);
        }
    }
}

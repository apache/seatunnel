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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * One real SeaTunnel member running {@code pipelineNum} real checkpoint coordinators, one per job,
 * whose periodic triggers the benchmark observes one at a time.
 *
 * <p>The coordinators are production code end to end; only the tasks are fakes, see {@link
 * FakeTaskCheckpointManager}. Whichever checkpoint scheduler the engine on the classpath uses is
 * the one being measured, so the same fixture runs unchanged on every revision being compared.
 *
 * <p>A trigger is observed through the coordinator's {@code pendingCounter}, which goes from 0 to 1
 * when the trigger body creates a pending checkpoint. The time a trigger is due is derived from the
 * previous observation plus one interval, since the coordinator re-arms itself with that delay. A
 * coordinator whose previous trigger was not observed has no known phase: it is taken out of the
 * rotation, counted as a skip, and resynchronised later.
 *
 * <p>Only every {@code probeStride}-th coordinator is measured; the rest are load. The probes start
 * at least {@link #MIN_PROBE_SPACING_NANOS} apart, which keeps two probes from coming due within
 * one sample of each other even after their phases drift. Measuring every coordinator instead would
 * skip whichever trigger came due while another was being measured, and those are the triggers that
 * bunch up under contention, so the skips would bias the result towards short delays exactly where
 * the scheduler is under the most pressure. Which coordinators are probes is fixed by index before
 * anything is measured, so probe samples carry no such selection, and probe triggers still collide
 * freely with the load.
 */
final class CheckpointSchedulingFixture {

    /** Covers the per-pipeline pools and the member-wide scheduler threads alike. */
    static final String SCHEDULER_THREAD_NAME_PREFIX = "checkpoint-";

    /**
     * The coordinator has no getter for its count of in-flight checkpoints. It is read, never
     * written, to see when a trigger has created a pending checkpoint. The trigger body increments
     * it just before re-arming the next trigger, which is what makes "observed + interval" the next
     * due time. If the field is renamed or removed, loading this class fails naming it.
     */
    private static final Field PENDING_COUNTER_FIELD =
            BenchmarkReflection.requireField(CheckpointCoordinator.class, "pendingCounter");

    /** Lowest interval {@code CheckpointConfig} accepts. */
    static final long MIN_CHECKPOINT_INTERVAL_MILLIS = CheckpointConfig.MINIMAL_CHECKPOINT_TIME;

    /** Share of due triggers that may be skipped before an iteration is rejected. */
    static final double MAX_SKIP_RATIO = 0.1;

    private static final int PIPELINE_ID = 1;
    private static final long FIRST_JOB_ID = 1_000L;
    private static final long NOT_SYNCED = Long.MIN_VALUE;

    /**
     * Bounds of how long before a due trigger the setup stops parking and starts spinning. Parking
     * alone would add the OS timer slack to the start of the measured window, and parking wakes
     * late by that slack: tens of microseconds on Linux, about 5 ms on macOS. The window starts at
     * the minimum and widens to the largest overshoot seen plus the minimum, up to the maximum.
     */
    private static final long MIN_SPIN_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private static final long MAX_SPIN_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Resync waits at most this many intervals, plus {@link #PENDING_REARM_NANOS}, for every
     * coordinator to trigger once.
     */
    private static final int RESYNC_INTERVALS = 3;

    /**
     * A trigger that finds a checkpoint still pending re-arms itself after 500 ms instead of one
     * interval; resync allows for one such re-arm, with margin.
     */
    private static final long PENDING_REARM_NANOS = TimeUnit.SECONDS.toNanos(1);

    /** Least spacing between the start phases of two measured coordinators. */
    private static final long MIN_PROBE_SPACING_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    private static final long EXECUTOR_KEEP_ALIVE_SECONDS = 60L;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30L;

    private final int pipelineNum;
    private final long intervalMillis;
    private final long intervalNanos;
    private int probeStride;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<FakeTaskCheckpointManager> managers = new ArrayList<>();

    private SeaTunnelStorageEnvironmentContext environment;
    private ThreadPoolExecutor coordinatorExecutor;
    private AtomicInteger[] pendingCounters;
    private long[] lastTriggerNanos;

    private long spinWindowNanos = MIN_SPIN_WINDOW_NANOS;
    private int current = -1;
    private long sampled;
    private long skippedPending;
    private long skippedCollided;
    private long skippedOverrun;
    private long skippedEarly;

    /**
     * @param pipelineNum number of jobs, each with one single-task pipeline
     * @param intervalMillis checkpoint interval of every job
     */
    CheckpointSchedulingFixture(int pipelineNum, long intervalMillis) {
        this.pipelineNum = pipelineNum;
        this.intervalMillis = intervalMillis;
        this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
    }

    /**
     * Starts the member, creates the coordinators and starts their pipelines staggered across one
     * interval, so due triggers arrive as a steady stream rather than a burst. Returns once every
     * coordinator's trigger phase is known.
     */
    void setUp() throws Exception {
        validateParameters();
        probeStride = probeStride(pipelineNum, intervalNanos);
        environment = new SchedulingEnvironmentContext();
        environment.setUp();
        SeaTunnelServer server = environment.getServer();
        EngineConfig engineConfig = server.getSeaTunnelConfig().getEngineConfig();
        coordinatorExecutor = createCoordinatorExecutor(engineConfig);
        createManagers(server, engineConfig.getCheckpointConfig());
        startStaggered();
        resync();
    }

    /** Resynchronises the coordinators that were taken out of the rotation, then resets counts. */
    void beginIteration() {
        resync();
        sampled = 0;
        skippedPending = 0;
        skippedCollided = 0;
        skippedOverrun = 0;
        skippedEarly = 0;
    }

    /**
     * Picks the coordinator due soonest and returns exactly when its trigger is due, with its
     * previous checkpoint completed and the trigger not yet run. Not measured.
     */
    void awaitNextDueTrigger() {
        checkFailure();
        while (true) {
            int next = soonestSynced();
            long expected = lastTriggerNanos[next] + intervalNanos;
            long parkTarget = expected - spinWindowNanos;
            long start = System.nanoTime();
            if (start >= expected) {
                // It came due while the previous sample was being taken; it may have run
                // unobserved, so its phase is lost.
                skipCollided(next);
                continue;
            }
            // Close enough to the due time already (the previous sample ended late in this
            // trigger's window): skip parking and go straight to the spin.
            if (start < parkTarget) {
                parkUntil(parkTarget);
                widenSpinWindow(System.nanoTime() - parkTarget);
            }
            // Counter first, then the clock: if the clock is still before the due time, the
            // counter was read before the trigger could have run.
            boolean pending = pendingCounters[next].get() != 0;
            if (System.nanoTime() >= expected) {
                // Parking overshot the due time; the trigger may have run unobserved.
                skipOverrun(next);
                continue;
            }
            if (pending) {
                // A checkpoint is pending before the trigger is due. Usually the previous one is
                // still running and this trigger takes the pending re-arm path; it can also be this
                // trigger having run early against an estimate made from a late observation, when
                // the measuring thread was descheduled. Either way the sample is unusable.
                skipPending(next);
                continue;
            }
            while (System.nanoTime() < expected) {
                // Spin through the last stretch so the measured window starts on time.
            }
            if (pendingCounters[next].get() != 0) {
                // The counter read 0 before the due time and only a trigger raises it, so the
                // trigger ran no later than its estimated due time: the estimate was late.
                skipEarly(next);
                continue;
            }
            current = next;
            return;
        }
    }

    /**
     * Spins until the due coordinator's trigger has created its pending checkpoint. This is the
     * measured part: it starts when the trigger is due and ends when the trigger has run.
     *
     * @return the time the trigger was observed
     */
    long awaitTrigger() {
        AtomicInteger pendingCounter = pendingCounters[current];
        long deadline = System.nanoTime() + intervalNanos;
        // The window starts slightly before the real deadline, and that is intended. The trigger
        // body increments pendingCounter just before it re-arms the next trigger with
        // schedule(..., interval), so "observed + interval" is a few microseconds early and each
        // sample also includes the tail of the previous trigger body. That code is identical on
        // every scheduler being compared, so the offset is equal on both sides and cancels in a
        // comparison. Do not "fix" it by moving the start later: there is no observable point
        // closer to the real deadline without changing engine code.
        while (pendingCounter.get() == 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "Checkpoint trigger of job "
                                + (FIRST_JOB_ID + current)
                                + " did not run within one interval of being due");
            }
        }
        long observed = System.nanoTime();
        lastTriggerNanos[current] = observed;
        sampled++;
        return observed;
    }

    /**
     * Summarises the iteration's skips. Printed for every iteration, passing or not, so a run close
     * to the limit stays visible.
     */
    String iterationReport() {
        return String.format(
                "measured %d of %d due triggers; skipped %d with a checkpoint already pending "
                        + "before the due time, %d that came due while another trigger was being "
                        + "measured, %d where parking overran the due time, %d that ran before "
                        + "their estimated due time",
                sampled,
                dueTriggers(),
                skippedPending,
                skippedCollided,
                skippedOverrun,
                skippedEarly);
    }

    /**
     * Rejects the iteration if more than {@link #MAX_SKIP_RATIO} of due triggers could not be
     * measured, so a run where most triggers took the pending re-arm path produces an error rather
     * than a number.
     */
    void endIteration() {
        checkFailure();
        long due = dueTriggers();
        long skipped = due - sampled;
        if (due > 0 && skipped > MAX_SKIP_RATIO * due) {
            throw new IllegalStateException(
                    String.format(
                            "%d pipelines: %s. That is above the %.0f%% limit, so the measured "
                                    + "delays would not be representative",
                            pipelineNum, iterationReport(), MAX_SKIP_RATIO * 100));
        }
    }

    private long dueTriggers() {
        return sampled + skippedPending + skippedCollided + skippedOverrun + skippedEarly;
    }

    void tearDown() throws Exception {
        try {
            // Executor first: a coordinator still finishing its asynchronous start would otherwise
            // arm its first trigger on the scheduler that cancelling just replaced, and that
            // thread would outlive the fixture.
            if (coordinatorExecutor != null) {
                coordinatorExecutor.shutdownNow();
                if (!coordinatorExecutor.awaitTermination(
                        SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Coordinator executor did not stop");
                }
            }
            for (FakeTaskCheckpointManager manager : managers) {
                manager.cancelCheckpoint(PIPELINE_ID);
            }
            managers.clear();
        } finally {
            coordinatorExecutor = null;
            if (environment != null) {
                environment.tearDown();
                environment = null;
            }
        }
    }

    long getSampled() {
        return sampled;
    }

    /**
     * Counts the live checkpoint scheduler threads by name. This is the cost a shared scheduler
     * exists to remove, so it is reported rather than derived.
     */
    static long countSchedulerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(SCHEDULER_THREAD_NAME_PREFIX))
                .count();
    }

    private void createManagers(SeaTunnelServer server, CheckpointConfig memberConfig) {
        NodeEngine nodeEngine = server.getNodeEngine();
        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        CheckpointConfig jobConfig = new CheckpointConfig();
        jobConfig.setCheckpointInterval(intervalMillis);
        jobConfig.setStorage(memberConfig.getStorage());

        pendingCounters = new AtomicInteger[pipelineNum];
        lastTriggerNanos = new long[pipelineNum];
        for (int i = 0; i < pipelineNum; i++) {
            long jobId = FIRST_JOB_ID + i;
            FakeTaskCheckpointManager manager =
                    new FakeTaskCheckpointManager(
                            jobId,
                            nodeEngine,
                            singleTaskPlan(jobId),
                            jobConfig,
                            server.getCheckpointService().getCheckpointStorage(),
                            coordinatorExecutor,
                            runningJobState,
                            server.getEngineContext(),
                            server.getCheckpointMonitorService(),
                            failure);
            managers.add(manager);
            pendingCounters[i] = readPendingCounter(manager.getCheckpointCoordinator(PIPELINE_ID));
            lastTriggerNanos[i] = NOT_SYNCED;
        }
    }

    private void startStaggered() {
        long start = System.nanoTime();
        for (int i = 0; i < pipelineNum; i++) {
            parkUntil(start + intervalNanos * i / pipelineNum);
            managers.get(i).startTask();
        }
    }

    /**
     * Spins over the probes until each one without a known phase has been seen idle and then
     * triggering. Probes that already have a phase are refreshed whenever they trigger during the
     * wait, so they do not fall out of the rotation meanwhile. Not measured; it occupies one core
     * for up to a few intervals.
     */
    private void resync() {
        boolean[] seenIdle = new boolean[pipelineNum];
        long deadline = System.nanoTime() + RESYNC_INTERVALS * intervalNanos + PENDING_REARM_NANOS;
        long unsynced = probeCount() - syncedCount();
        while (unsynced > 0) {
            checkFailure();
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        unsynced
                                + " of "
                                + probeCount()
                                + " measured checkpoint coordinators did not trigger within "
                                + RESYNC_INTERVALS
                                + " intervals plus one pending re-arm");
            }
            for (int i = 0; i < pipelineNum; i += probeStride) {
                if (pendingCounters[i].get() == 0) {
                    seenIdle[i] = true;
                } else if (seenIdle[i]) {
                    if (lastTriggerNanos[i] == NOT_SYNCED) {
                        unsynced--;
                    }
                    lastTriggerNanos[i] = System.nanoTime();
                    seenIdle[i] = false;
                }
            }
        }
    }

    /**
     * Returns the probe with a known phase that is due soonest. Resynchronises first once fewer
     * than half the probes have a known phase: skipped probes would otherwise stay out of the
     * rotation for the rest of the iteration, and with few probes a single skip can leave none.
     */
    private int soonestSynced() {
        if (syncedCount() * 2 < probeCount()) {
            resync();
        }
        int soonest = -1;
        for (int i = 0; i < pipelineNum; i += probeStride) {
            if (lastTriggerNanos[i] != NOT_SYNCED
                    && (soonest < 0 || lastTriggerNanos[i] < lastTriggerNanos[soonest])) {
                soonest = i;
            }
        }
        return soonest;
    }

    long probeCount() {
        return (pipelineNum + probeStride - 1) / probeStride;
    }

    private long syncedCount() {
        long synced = 0;
        for (int i = 0; i < pipelineNum; i += probeStride) {
            if (lastTriggerNanos[i] != NOT_SYNCED) {
                synced++;
            }
        }
        return synced;
    }

    private void widenSpinWindow(long parkOvershootNanos) {
        spinWindowNanos =
                Math.min(
                        MAX_SPIN_WINDOW_NANOS,
                        Math.max(spinWindowNanos, parkOvershootNanos + MIN_SPIN_WINDOW_NANOS));
    }

    private void skipPending(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedPending++;
    }

    private void skipCollided(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedCollided++;
    }

    private void skipOverrun(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedOverrun++;
    }

    private void skipEarly(int coordinator) {
        lastTriggerNanos[coordinator] = NOT_SYNCED;
        skippedEarly++;
    }

    private void checkFailure() {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throw new IllegalStateException("Checkpoint coordinator failed", throwable);
        }
    }

    private void validateParameters() {
        if (pipelineNum < 1) {
            throw new IllegalArgumentException("pipelineNum must be at least 1");
        }
        if (intervalMillis < MIN_CHECKPOINT_INTERVAL_MILLIS) {
            throw new IllegalArgumentException(
                    "checkpoint interval must be at least "
                            + MIN_CHECKPOINT_INTERVAL_MILLIS
                            + " ms to match the minimum SeaTunnel accepts");
        }
    }

    /**
     * Starts are spread evenly across one interval, so coordinators {@code i} and {@code i +
     * stride} start {@code stride * interval / pipelineNum} apart. Returns the smallest stride that
     * keeps that at least {@link #MIN_PROBE_SPACING_NANOS}, and at most {@code pipelineNum} so
     * there is always one probe.
     */
    static int probeStride(int pipelineNum, long intervalNanos) {
        long stride = (MIN_PROBE_SPACING_NANOS * pipelineNum + intervalNanos - 1) / intervalNanos;
        return (int) Math.max(1L, Math.min(stride, pipelineNum));
    }

    private static CheckpointPlan singleTaskPlan(long jobId) {
        TaskLocation task = new TaskLocation(new TaskGroupLocation(jobId, PIPELINE_ID, 1L), 0, 0);
        return CheckpointPlan.builder()
                .pipelineId(PIPELINE_ID)
                .pipelineSubtasks(Collections.singleton(task))
                .startingSubtasks(Collections.singleton(task))
                .pipelineActions(Collections.emptyMap())
                .subtaskActions(Collections.emptyMap())
                .build();
    }

    /** Same shape as the executor {@code CoordinatorService} hands every {@code JobMaster}. */
    private static ThreadPoolExecutor createCoordinatorExecutor(EngineConfig engineConfig) {
        AtomicInteger threadIndex = new AtomicInteger();
        return new ThreadPoolExecutor(
                engineConfig.getCoordinatorServiceConfig().getCoreThreadNum(),
                engineConfig.getCoordinatorServiceConfig().getMaxThreadNum(),
                EXECUTOR_KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName(
                            "benchmark-coordinator-service-" + threadIndex.getAndIncrement());
                    return thread;
                });
    }

    private static AtomicInteger readPendingCounter(CheckpointCoordinator coordinator) {
        try {
            return (AtomicInteger) PENDING_COUNTER_FIELD.get(coordinator);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read the pending checkpoint counter", e);
        }
    }

    /**
     * The storage benchmarks' member, with its checkpoint storage isolated in the trial's temporary
     * directory, but without IMap persistence. Its write-through MapStore puts a file write on a
     * partition thread for every engine IMap update, including checkpoint id allocation, which
     * stalls checkpoints for hundreds of milliseconds and is not what this benchmark measures.
     */
    private static final class SchedulingEnvironmentContext
            extends SeaTunnelStorageEnvironmentContext {

        private static final String ENGINE_MAPS = "engine*";

        @Override
        protected SeaTunnelConfig createSeaTunnelConfig(String clusterName) {
            SeaTunnelConfig config = super.createSeaTunnelConfig(clusterName);
            config.getHazelcastConfig()
                    .getMapConfig(ENGINE_MAPS)
                    .getMapStoreConfig()
                    .setEnabled(false);
            return config;
        }
    }

    private static void parkUntil(long deadlineNanos) {
        long remaining;
        while ((remaining = deadlineNanos - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
    }
}

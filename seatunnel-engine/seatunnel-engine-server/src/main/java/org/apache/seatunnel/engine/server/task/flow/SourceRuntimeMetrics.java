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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;

import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_BARRIER_FORWARD_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_BARRIER_FORWARD_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_LOCK_WAIT_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_LOCK_WAIT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_SNAPSHOT_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_SNAPSHOT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_SNAPSHOT_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_TOTAL;

/**
 * Aggregates bounded-cardinality timing metrics for the current Source runtime.
 *
 * <p>The legacy reader path and the future managed mailbox path must report the same timing
 * boundaries so that rollout decisions compare equivalent data. This class intentionally records
 * counters only; it does not schedule work or change SourceReader callback ordering.
 */
final class SourceRuntimeMetrics {

    /**
     * Initial soft budget for one SourceReader poll turn.
     *
     * <p>Phase 0 uses this threshold to measure the population that cannot yet cooperate with a
     * mailbox runtime. It is diagnostic only and never interrupts a legacy reader.
     */
    static final long POLL_SOFT_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(5L);

    /**
     * Maximum time a reader control callback should occupy an operation thread.
     *
     * <p>The threshold is diagnostic only. Legacy callbacks continue to completion even when they
     * exceed it.
     */
    static final long READER_CALLBACK_SOFT_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(5L);

    /** Total poll invocation counter. */
    private final Counter pollTotal;

    /** Cumulative poll wall time. */
    private final Counter pollNanos;

    /** Maximum observed poll wall time. */
    private final Counter pollMaxNanos;

    /** Poll soft-budget violation counter. */
    private final Counter pollBudgetExceededTotal;

    /** Total reader control callback invocation counter. */
    private final Counter readerCallbackTotal;

    /** Cumulative reader control callback wall time. */
    private final Counter readerCallbackNanos;

    /** Maximum observed reader control callback wall time. */
    private final Counter readerCallbackMaxNanos;

    /** Reader control callback soft-budget violation counter. */
    private final Counter readerCallbackBudgetExceededTotal;

    /** Total source checkpoint barrier counter. */
    private final Counter checkpointTotal;

    /** Cumulative checkpoint-lock wait time. */
    private final Counter checkpointLockWaitNanos;

    /** Maximum observed checkpoint-lock wait time. */
    private final Counter checkpointLockWaitMaxNanos;

    /** Cumulative SourceReader snapshot and state-registration time. */
    private final Counter checkpointSnapshotNanos;

    /** Total SourceReader state snapshot counter. */
    private final Counter checkpointSnapshotTotal;

    /** Maximum observed SourceReader snapshot and state-registration time. */
    private final Counter checkpointSnapshotMaxNanos;

    /** Cumulative barrier acknowledgment and downstream forwarding time. */
    private final Counter barrierForwardNanos;

    /** Maximum observed barrier acknowledgment and downstream forwarding time. */
    private final Counter barrierForwardMaxNanos;

    /**
     * Registers all Source runtime metrics for one source action and task attempt.
     *
     * @param metricsContext task metrics registry
     * @param sourceRuntimeId stable source action identifier within the job
     * @param executionId immutable engine deployment identity for this task attempt
     */
    SourceRuntimeMetrics(MetricsContext metricsContext, long sourceRuntimeId, long executionId) {
        String suffix = metricSuffix(sourceRuntimeId, executionId);
        this.pollTotal = metricsContext.counter(SOURCE_POLL_TOTAL + suffix);
        this.pollNanos = metricsContext.counter(SOURCE_POLL_NANOS + suffix);
        this.pollMaxNanos = metricsContext.counter(SOURCE_POLL_MAX_NANOS + suffix);
        this.pollBudgetExceededTotal =
                metricsContext.counter(SOURCE_POLL_BUDGET_EXCEEDED_TOTAL + suffix);
        this.readerCallbackTotal = metricsContext.counter(SOURCE_READER_CALLBACK_TOTAL + suffix);
        this.readerCallbackNanos = metricsContext.counter(SOURCE_READER_CALLBACK_NANOS + suffix);
        this.readerCallbackMaxNanos =
                metricsContext.counter(SOURCE_READER_CALLBACK_MAX_NANOS + suffix);
        this.readerCallbackBudgetExceededTotal =
                metricsContext.counter(SOURCE_READER_CALLBACK_BUDGET_EXCEEDED_TOTAL + suffix);
        this.checkpointTotal = metricsContext.counter(SOURCE_CHECKPOINT_TOTAL + suffix);
        this.checkpointLockWaitNanos =
                metricsContext.counter(SOURCE_CHECKPOINT_LOCK_WAIT_NANOS + suffix);
        this.checkpointLockWaitMaxNanos =
                metricsContext.counter(SOURCE_CHECKPOINT_LOCK_WAIT_MAX_NANOS + suffix);
        this.checkpointSnapshotTotal =
                metricsContext.counter(SOURCE_CHECKPOINT_SNAPSHOT_TOTAL + suffix);
        this.checkpointSnapshotNanos =
                metricsContext.counter(SOURCE_CHECKPOINT_SNAPSHOT_NANOS + suffix);
        this.checkpointSnapshotMaxNanos =
                metricsContext.counter(SOURCE_CHECKPOINT_SNAPSHOT_MAX_NANOS + suffix);
        this.barrierForwardNanos = metricsContext.counter(SOURCE_BARRIER_FORWARD_NANOS + suffix);
        this.barrierForwardMaxNanos =
                metricsContext.counter(SOURCE_BARRIER_FORWARD_MAX_NANOS + suffix);
    }

    /**
     * Builds a bounded-cardinality suffix for runtime metrics introduced by the rollout lane.
     *
     * @param sourceRuntimeId stable source action identifier within the job
     * @param executionId immutable engine deployment identity for this task attempt
     * @return metric suffix shared by all Source runtime counters
     */
    static String metricSuffix(long sourceRuntimeId, long executionId) {
        return "#" + sourceRuntimeId + "#attempt-" + executionId;
    }

    /**
     * Records one SourceReader poll invocation.
     *
     * @param elapsedNanos complete wall time spent inside the callback
     */
    void recordPoll(long elapsedNanos) {
        pollTotal.inc();
        pollNanos.inc(elapsedNanos);
        updateMax(pollMaxNanos, elapsedNanos);
        if (elapsedNanos > POLL_SOFT_BUDGET_NANOS) {
            pollBudgetExceededTotal.inc();
        }
    }

    /**
     * Records one SourceReader control callback executed outside the reader loop.
     *
     * @param elapsedNanos complete wall time spent inside the callback
     */
    void recordReaderCallback(long elapsedNanos) {
        readerCallbackTotal.inc();
        readerCallbackNanos.inc(elapsedNanos);
        updateMax(readerCallbackMaxNanos, elapsedNanos);
        if (elapsedNanos > READER_CALLBACK_SOFT_BUDGET_NANOS) {
            readerCallbackBudgetExceededTotal.inc();
        }
    }

    /**
     * Records how long a checkpoint barrier waited before entering the checkpoint critical section.
     *
     * @param elapsedNanos wall time between barrier admission and lock acquisition
     */
    void recordCheckpointLockWait(long elapsedNanos) {
        checkpointTotal.inc();
        checkpointLockWaitNanos.inc(elapsedNanos);
        updateMax(checkpointLockWaitMaxNanos, elapsedNanos);
    }

    /**
     * Records SourceReader snapshot serialization and state registration time.
     *
     * @param elapsedNanos wall time for the complete state snapshot stage
     */
    void recordCheckpointSnapshot(long elapsedNanos) {
        checkpointSnapshotTotal.inc();
        checkpointSnapshotNanos.inc(elapsedNanos);
        updateMax(checkpointSnapshotMaxNanos, elapsedNanos);
    }

    /**
     * Records checkpoint acknowledgment and downstream barrier forwarding time.
     *
     * @param elapsedNanos wall time for the complete forward stage
     */
    void recordBarrierForward(long elapsedNanos) {
        barrierForwardNanos.inc(elapsedNanos);
        updateMax(barrierForwardMaxNanos, elapsedNanos);
    }

    /**
     * Updates a counter used as a maximum gauge without allowing concurrent lower values to
     * overwrite a higher observation.
     *
     * @param maximumCounter counter that exposes the maximum observation
     * @param candidate observed duration
     */
    private static void updateMax(Counter maximumCounter, long candidate) {
        if (candidate <= maximumCounter.getCount()) {
            return;
        }
        synchronized (maximumCounter) {
            if (candidate > maximumCounter.getCount()) {
                maximumCounter.set(candidate);
            }
        }
    }
}

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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ADMISSION_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ADMISSION_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ADMISSION_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ADMISSION_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_APPLIED_WATERMARK;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASSIGNMENT_BACKPRESSURE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASSIGNMENT_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASSIGNMENT_COMPACTION_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASSIGNMENT_ENTRIES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASSIGNMENT_OLDEST_AGE_MILLIS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASSIGNMENT_STATE_ENTRIES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_CALLBACKS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_COALESCED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_EXECUTION_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_QUEUE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_RUNNING;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_SKIPPED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_STALE_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_TIMEOUT_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ASYNC_WAITING;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_COLLECT_BLOCKED_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_COMMAND_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_COMMAND_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_COMMAND_QUEUE_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_COMMAND_QUEUE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_COMMAND_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_DEDUPE_GAPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_MAILBOX_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_MAILBOX_COMMANDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_MAILBOX_OLDEST_AGE_MILLIS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_OUTBOUND_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_OUTBOUND_COMMANDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_POLL_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_POLL_RECORDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_REGISTRATION_RECONCILIATION_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_RESERVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_RESERVED_COMMANDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_TRANSPORT_RETRY_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_WAKEUP_TIMEOUT_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_WAKEUP_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_TOTAL;

/**
 * Bounded-cardinality metrics shared by managed Source Reader and coordinator runtimes.
 *
 * <p>SeaTunnel's metrics API represents gauges with counters whose current value is set. Every
 * suffix used here comes from a fixed enum or the stable source action identifier; command IDs,
 * split IDs, table names, and exception text never become metric names.
 */
final class ManagedSourceRuntimeMetrics {
    private final Map<SourceCommandAdmissionStatus, Counter> admissionCounters =
            new EnumMap<>(SourceCommandAdmissionStatus.class);
    private final Map<SourceAssignmentState, Counter> assignmentStateCounters =
            new EnumMap<>(SourceAssignmentState.class);

    private final Counter mailboxCommands;
    private final Counter mailboxBytes;
    private final Counter mailboxOldestAgeMillis;
    private final Counter reservedCommands;
    private final Counter reservedBytes;
    private final Counter outboundCommands;
    private final Counter outboundBytes;
    private final Counter admissionNanos;
    private final Counter admissionMaxNanos;
    private final Counter admissionBudgetExceeded;
    private final Counter commandTotal;
    private final Counter commandQueueNanos;
    private final Counter commandQueueMaxNanos;
    private final Counter commandNanos;
    private final Counter commandMaxNanos;
    private final Counter transportRetryTotal;
    private final Counter pollTotal;
    private final Counter pollNanos;
    private final Counter pollMaxNanos;
    private final Counter pollBudgetExceeded;
    private final Counter pollRecords;
    private final Counter pollBytes;
    private final Counter wakeupTotal;
    private final Counter wakeupTimeoutTotal;
    private final Counter appliedWatermark;
    private final Counter dedupeGaps;
    private final Counter collectBlockedNanos;
    private final Counter assignmentEntries;
    private final Counter assignmentBytes;
    private final Counter assignmentOldestAgeMillis;
    private final Counter assignmentCompactionTotal;
    private final Counter assignmentBackpressureNanos;
    private final Counter registrationReconciliationNanos;
    private final Counter asyncRunning;
    private final Counter asyncWaiting;
    private final Counter asyncCallbacks;
    private final Counter asyncTimeoutTotal;
    private final Counter asyncCoalescedTotal;
    private final Counter asyncSkippedTotal;
    private final Counter asyncStaleTotal;
    private final Counter asyncQueueNanos;
    private final Counter asyncExecutionNanos;

    ManagedSourceRuntimeMetrics(MetricsContext context, long sourceRuntimeId) {
        String suffix = "#" + sourceRuntimeId;
        mailboxCommands = context.counter(SOURCE_MANAGED_MAILBOX_COMMANDS + suffix);
        mailboxBytes = context.counter(SOURCE_MANAGED_MAILBOX_BYTES + suffix);
        mailboxOldestAgeMillis = context.counter(SOURCE_MANAGED_MAILBOX_OLDEST_AGE_MILLIS + suffix);
        reservedCommands = context.counter(SOURCE_MANAGED_RESERVED_COMMANDS + suffix);
        reservedBytes = context.counter(SOURCE_MANAGED_RESERVED_BYTES + suffix);
        outboundCommands = context.counter(SOURCE_MANAGED_OUTBOUND_COMMANDS + suffix);
        outboundBytes = context.counter(SOURCE_MANAGED_OUTBOUND_BYTES + suffix);
        admissionNanos = context.counter(SOURCE_MANAGED_ADMISSION_NANOS + suffix);
        admissionMaxNanos = context.counter(SOURCE_MANAGED_ADMISSION_MAX_NANOS + suffix);
        admissionBudgetExceeded =
                context.counter(SOURCE_MANAGED_ADMISSION_BUDGET_EXCEEDED_TOTAL + suffix);
        commandTotal = context.counter(SOURCE_MANAGED_COMMAND_TOTAL + suffix);
        commandQueueNanos = context.counter(SOURCE_MANAGED_COMMAND_QUEUE_NANOS + suffix);
        commandQueueMaxNanos = context.counter(SOURCE_MANAGED_COMMAND_QUEUE_MAX_NANOS + suffix);
        commandNanos = context.counter(SOURCE_MANAGED_COMMAND_NANOS + suffix);
        commandMaxNanos = context.counter(SOURCE_MANAGED_COMMAND_MAX_NANOS + suffix);
        transportRetryTotal = context.counter(SOURCE_MANAGED_TRANSPORT_RETRY_TOTAL + suffix);
        pollTotal = context.counter(SOURCE_POLL_TOTAL + suffix);
        pollNanos = context.counter(SOURCE_POLL_NANOS + suffix);
        pollMaxNanos = context.counter(SOURCE_POLL_MAX_NANOS + suffix);
        pollBudgetExceeded = context.counter(SOURCE_POLL_BUDGET_EXCEEDED_TOTAL + suffix);
        pollRecords = context.counter(SOURCE_MANAGED_POLL_RECORDS + suffix);
        pollBytes = context.counter(SOURCE_MANAGED_POLL_BYTES + suffix);
        wakeupTotal = context.counter(SOURCE_MANAGED_WAKEUP_TOTAL + suffix);
        wakeupTimeoutTotal = context.counter(SOURCE_MANAGED_WAKEUP_TIMEOUT_TOTAL + suffix);
        appliedWatermark = context.counter(SOURCE_MANAGED_APPLIED_WATERMARK + suffix);
        dedupeGaps = context.counter(SOURCE_MANAGED_DEDUPE_GAPS + suffix);
        collectBlockedNanos = context.counter(SOURCE_MANAGED_COLLECT_BLOCKED_NANOS + suffix);
        assignmentEntries = context.counter(SOURCE_MANAGED_ASSIGNMENT_ENTRIES + suffix);
        assignmentBytes = context.counter(SOURCE_MANAGED_ASSIGNMENT_BYTES + suffix);
        assignmentOldestAgeMillis =
                context.counter(SOURCE_MANAGED_ASSIGNMENT_OLDEST_AGE_MILLIS + suffix);
        assignmentCompactionTotal =
                context.counter(SOURCE_MANAGED_ASSIGNMENT_COMPACTION_TOTAL + suffix);
        assignmentBackpressureNanos =
                context.counter(SOURCE_MANAGED_ASSIGNMENT_BACKPRESSURE_NANOS + suffix);
        registrationReconciliationNanos =
                context.counter(SOURCE_MANAGED_REGISTRATION_RECONCILIATION_NANOS + suffix);
        asyncRunning = context.counter(SOURCE_MANAGED_ASYNC_RUNNING + suffix);
        asyncWaiting = context.counter(SOURCE_MANAGED_ASYNC_WAITING + suffix);
        asyncCallbacks = context.counter(SOURCE_MANAGED_ASYNC_CALLBACKS + suffix);
        asyncTimeoutTotal = context.counter(SOURCE_MANAGED_ASYNC_TIMEOUT_TOTAL + suffix);
        asyncCoalescedTotal = context.counter(SOURCE_MANAGED_ASYNC_COALESCED_TOTAL + suffix);
        asyncSkippedTotal = context.counter(SOURCE_MANAGED_ASYNC_SKIPPED_TOTAL + suffix);
        asyncStaleTotal = context.counter(SOURCE_MANAGED_ASYNC_STALE_TOTAL + suffix);
        asyncQueueNanos = context.counter(SOURCE_MANAGED_ASYNC_QUEUE_NANOS + suffix);
        asyncExecutionNanos = context.counter(SOURCE_MANAGED_ASYNC_EXECUTION_NANOS + suffix);
        for (SourceCommandAdmissionStatus status : SourceCommandAdmissionStatus.values()) {
            admissionCounters.put(
                    status,
                    context.counter(SOURCE_MANAGED_ADMISSION_TOTAL + suffix + "#" + status.name()));
        }
        for (SourceAssignmentState state : SourceAssignmentState.values()) {
            assignmentStateCounters.put(
                    state,
                    context.counter(
                            SOURCE_MANAGED_ASSIGNMENT_STATE_ENTRIES + suffix + "#" + state.name()));
        }
    }

    void recordAdmission(SourceCommandAdmissionStatus status) {
        admissionCounters.get(status).inc();
    }

    void recordAdmissionDuration(long elapsedNanos, long budgetNanos) {
        admissionNanos.inc(elapsedNanos);
        updateMax(admissionMaxNanos, elapsedNanos);
        if (elapsedNanos > budgetNanos) {
            admissionBudgetExceeded.inc();
        }
    }

    void recordCommand(long queueNanos, long serviceNanos) {
        commandTotal.inc();
        commandQueueNanos.inc(queueNanos);
        updateMax(commandQueueMaxNanos, queueNanos);
        commandNanos.inc(serviceNanos);
        updateMax(commandMaxNanos, serviceNanos);
    }

    void recordTransportRetry() {
        transportRetryTotal.inc();
    }

    void recordPoll(
            long elapsedNanos, int emittedRecords, long emittedBytes, boolean budgetExceeded) {
        pollTotal.inc();
        pollNanos.inc(elapsedNanos);
        updateMax(pollMaxNanos, elapsedNanos);
        pollRecords.inc(emittedRecords);
        pollBytes.inc(emittedBytes);
        if (budgetExceeded) {
            pollBudgetExceeded.inc();
        }
    }

    void recordWakeup() {
        wakeupTotal.inc();
    }

    void recordWakeupTimeout() {
        wakeupTimeoutTotal.inc();
    }

    void recordCollectBlocked(long elapsedNanos) {
        collectBlockedNanos.inc(elapsedNanos);
    }

    void updateReaderState(
            ReaderCommandMailbox mailbox,
            int controlCommands,
            int reservedControlCommands,
            int outboundCommandCount,
            long outboundByteCount,
            long watermark,
            int gapCount) {
        mailboxCommands.set(controlCommands);
        mailboxBytes.set(mailbox.bytes());
        mailboxOldestAgeMillis.set(
                TimeUnit.NANOSECONDS.toMillis(mailbox.oldestCommandAgeNanos(System.nanoTime())));
        reservedCommands.set(reservedControlCommands);
        reservedBytes.set(mailbox.reservedBytes());
        outboundCommands.set(outboundCommandCount);
        outboundBytes.set(outboundByteCount);
        appliedWatermark.set(watermark);
        dedupeGaps.set(gapCount);
    }

    void updateCoordinatorState(
            int controlCommands,
            long mailboxByteCount,
            long oldestCommandAgeNanos,
            int reservedCommandCount,
            long reservedByteCount,
            int outboundCommandCount,
            long outboundByteCount,
            SourceAssignmentTracker tracker,
            ManagedCoordinatorScheduler scheduler) {
        mailboxCommands.set(controlCommands);
        mailboxBytes.set(mailboxByteCount);
        mailboxOldestAgeMillis.set(TimeUnit.NANOSECONDS.toMillis(oldestCommandAgeNanos));
        reservedCommands.set(reservedCommandCount);
        reservedBytes.set(reservedByteCount);
        outboundCommands.set(outboundCommandCount);
        outboundBytes.set(outboundByteCount);
        assignmentEntries.set(tracker.size());
        assignmentBytes.set(tracker.trackedBytes());
        assignmentOldestAgeMillis.set(
                tracker.oldestAssignmentAgeMillis(System.currentTimeMillis()));
        assignmentCompactionTotal.set(tracker.compactedEntries());
        for (SourceAssignmentState state : SourceAssignmentState.values()) {
            assignmentStateCounters.get(state).set(tracker.stateCount(state));
        }
        asyncRunning.set(scheduler.runningCount());
        asyncWaiting.set(scheduler.waitingCount());
        asyncCallbacks.set(scheduler.callbackCount());
        asyncTimeoutTotal.set(scheduler.timeoutCount());
        asyncCoalescedTotal.set(scheduler.coalescedCount());
        asyncSkippedTotal.set(scheduler.skippedCount());
        asyncStaleTotal.set(scheduler.staleCallbackCount());
        asyncQueueNanos.set(scheduler.queueNanos());
        asyncExecutionNanos.set(scheduler.executionNanos());
    }

    void recordAssignmentBackpressure(long elapsedNanos) {
        assignmentBackpressureNanos.inc(elapsedNanos);
    }

    void recordRegistrationReconciliation(long elapsedNanos) {
        registrationReconciliationNanos.inc(elapsedNanos);
    }

    private static void updateMax(Counter maximum, long candidate) {
        if (candidate <= maximum.getCount()) {
            return;
        }
        synchronized (maximum) {
            if (candidate > maximum.getCount()) {
                maximum.set(candidate);
            }
        }
    }
}

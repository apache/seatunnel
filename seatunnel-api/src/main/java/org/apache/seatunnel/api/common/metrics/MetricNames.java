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

package org.apache.seatunnel.api.common.metrics;

public final class MetricNames {

    private MetricNames() {}

    public static final String RECEIVED_COUNT = "receivedCount";

    public static final String RECEIVED_BATCHES = "receivedBatches";

    public static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";
    public static final String SOURCE_RECEIVED_BYTES = "SourceReceivedBytes";
    public static final String SOURCE_RECEIVED_QPS = "SourceReceivedQPS";
    public static final String SOURCE_RECEIVED_BYTES_PER_SECONDS = "SourceReceivedBytesPerSeconds";
    public static final String SINK_WRITE_COUNT = "SinkWriteCount";
    public static final String SINK_WRITE_BYTES = "SinkWriteBytes";
    public static final String SINK_WRITE_QPS = "SinkWriteQPS";
    public static final String SINK_WRITE_BYTES_PER_SECONDS = "SinkWriteBytesPerSeconds";
    public static final String SINK_COMMITTED_COUNT = "SinkCommittedCount";
    public static final String SINK_COMMITTED_BYTES = "SinkCommittedBytes";
    public static final String SINK_COMMITTED_QPS = "SinkCommittedQPS";
    public static final String SINK_COMMITTED_BYTES_PER_SECONDS = "SinkCommittedBytesPerSeconds";

    public static final String INTERMEDIATE_QUEUE_SIZE = "IntermediateQueueSize";

    /** Total nanoseconds spent blocked when putting into intermediate queues. */
    public static final String INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS =
            "IntermediateQueuePutBlockedNs";

    /** Capacity of intermediate queues (reported as a constant counter value). */
    public static final String INTERMEDIATE_QUEUE_CAPACITY = "IntermediateQueueCapacity";

    /** Total nanoseconds spent reading (polling with output) in Source. */
    public static final String SOURCE_READ_NANOS = "SourceReadNs";

    /** Total nanoseconds spent idle (polling empty / sleeping / waiting) in Source. */
    public static final String SOURCE_IDLE_NANOS = "SourceIdleNs";

    /** Total number of SourceReader poll invocations. */
    public static final String SOURCE_POLL_TOTAL = "SourcePollTotal";

    /** Total wall-clock nanoseconds spent in SourceReader poll invocations. */
    public static final String SOURCE_POLL_NANOS = "SourcePollNs";

    /** Maximum wall-clock nanoseconds observed for a SourceReader poll invocation. */
    public static final String SOURCE_POLL_MAX_NANOS = "SourcePollMaxNs";

    /** Total SourceReader poll invocations that exceeded the runtime soft budget. */
    public static final String SOURCE_POLL_BUDGET_EXCEEDED_TOTAL = "SourcePollBudgetExceededTotal";

    /** Total number of SourceReader control callback invocations. */
    public static final String SOURCE_READER_CALLBACK_TOTAL = "SourceReaderCallbackTotal";

    /** Total wall-clock nanoseconds spent in SourceReader control callbacks. */
    public static final String SOURCE_READER_CALLBACK_NANOS = "SourceReaderCallbackNs";

    /** Maximum wall-clock nanoseconds observed for a SourceReader control callback. */
    public static final String SOURCE_READER_CALLBACK_MAX_NANOS = "SourceReaderCallbackMaxNs";

    /** Total SourceReader control callbacks that exceeded the operation-thread soft budget. */
    public static final String SOURCE_READER_CALLBACK_BUDGET_EXCEEDED_TOTAL =
            "SourceReaderCallbackBudgetExceededTotal";

    /** Total number of source checkpoint barriers processed by a reader task. */
    public static final String SOURCE_CHECKPOINT_TOTAL = "SourceCheckpointTotal";

    /** Total nanoseconds source checkpoints waited to acquire the checkpoint lock. */
    public static final String SOURCE_CHECKPOINT_LOCK_WAIT_NANOS = "SourceCheckpointLockWaitNs";

    /** Maximum nanoseconds a source checkpoint waited to acquire the checkpoint lock. */
    public static final String SOURCE_CHECKPOINT_LOCK_WAIT_MAX_NANOS =
            "SourceCheckpointLockWaitMaxNs";

    /** Total number of SourceReader state snapshots executed by a reader task. */
    public static final String SOURCE_CHECKPOINT_SNAPSHOT_TOTAL = "SourceCheckpointSnapshotTotal";

    /** Total nanoseconds spent snapshotting and registering SourceReader state. */
    public static final String SOURCE_CHECKPOINT_SNAPSHOT_NANOS = "SourceCheckpointSnapshotNs";

    /** Maximum nanoseconds spent snapshotting and registering SourceReader state. */
    public static final String SOURCE_CHECKPOINT_SNAPSHOT_MAX_NANOS =
            "SourceCheckpointSnapshotMaxNs";

    /** Total nanoseconds spent acknowledging and forwarding source checkpoint barriers. */
    public static final String SOURCE_BARRIER_FORWARD_NANOS = "SourceBarrierForwardNs";

    /** Maximum nanoseconds spent acknowledging and forwarding a source checkpoint barrier. */
    public static final String SOURCE_BARRIER_FORWARD_MAX_NANOS = "SourceBarrierForwardMaxNs";

    /** Current command count in an engine-managed Source mailbox. */
    public static final String SOURCE_MANAGED_MAILBOX_COMMANDS = "SourceManagedMailboxCommands";

    /** Current payload bytes retained by an engine-managed Source mailbox. */
    public static final String SOURCE_MANAGED_MAILBOX_BYTES = "SourceManagedMailboxBytes";

    /** Current age in milliseconds of the oldest engine-managed Source command. */
    public static final String SOURCE_MANAGED_MAILBOX_OLDEST_AGE_MILLIS =
            "SourceManagedMailboxOldestAgeMs";

    /** Current command count retained by a managed Source outbound retry window. */
    public static final String SOURCE_MANAGED_OUTBOUND_COMMANDS = "SourceManagedOutboundCommands";

    /** Current bytes retained by a managed Source outbound retry window. */
    public static final String SOURCE_MANAGED_OUTBOUND_BYTES = "SourceManagedOutboundBytes";

    /** Current command count consuming reserved managed Source control capacity. */
    public static final String SOURCE_MANAGED_RESERVED_COMMANDS = "SourceManagedReservedCommands";

    /** Current bytes consuming reserved managed Source control capacity. */
    public static final String SOURCE_MANAGED_RESERVED_BYTES = "SourceManagedReservedBytes";

    /** Managed Source admission result counter prefix followed by a bounded status suffix. */
    public static final String SOURCE_MANAGED_ADMISSION_TOTAL = "SourceManagedAdmissionTotal";

    /** Cumulative time spent in managed Source admission paths. */
    public static final String SOURCE_MANAGED_ADMISSION_NANOS = "SourceManagedAdmissionNs";

    /** Maximum time spent in one managed Source admission path. */
    public static final String SOURCE_MANAGED_ADMISSION_MAX_NANOS = "SourceManagedAdmissionMaxNs";

    /** Total managed Source admissions exceeding the configured operation-thread budget. */
    public static final String SOURCE_MANAGED_ADMISSION_BUDGET_EXCEEDED_TOTAL =
            "SourceManagedAdmissionBudgetExceededTotal";

    /** Total managed Source commands executed by an event-loop owner. */
    public static final String SOURCE_MANAGED_COMMAND_TOTAL = "SourceManagedCommandTotal";

    /** Cumulative managed Source command queue wait time. */
    public static final String SOURCE_MANAGED_COMMAND_QUEUE_NANOS = "SourceManagedCommandQueueNs";

    /** Maximum managed Source command queue wait time. */
    public static final String SOURCE_MANAGED_COMMAND_QUEUE_MAX_NANOS =
            "SourceManagedCommandQueueMaxNs";

    /** Cumulative managed Source command service time. */
    public static final String SOURCE_MANAGED_COMMAND_NANOS = "SourceManagedCommandNs";

    /** Maximum managed Source command service time. */
    public static final String SOURCE_MANAGED_COMMAND_MAX_NANOS = "SourceManagedCommandMaxNs";

    /** Total managed Source transport retries. */
    public static final String SOURCE_MANAGED_TRANSPORT_RETRY_TOTAL =
            "SourceManagedTransportRetryTotal";

    /** Total records emitted by cooperative managed Source poll turns. */
    public static final String SOURCE_MANAGED_POLL_RECORDS = "SourceManagedPollRecords";

    /** Estimated payload bytes emitted by cooperative managed Source poll turns. */
    public static final String SOURCE_MANAGED_POLL_BYTES = "SourceManagedPollBytes";

    /** Total managed Source wakeup requests used to enforce hard poll bounds. */
    public static final String SOURCE_MANAGED_WAKEUP_TOTAL = "SourceManagedWakeupTotal";

    /** Total managed Source polls that exceeded the cancellation timeout after wakeup. */
    public static final String SOURCE_MANAGED_WAKEUP_TIMEOUT_TOTAL =
            "SourceManagedWakeupTimeoutTotal";

    /** Current applied sender sequence watermark of a managed Source Reader. */
    public static final String SOURCE_MANAGED_APPLIED_WATERMARK = "SourceManagedAppliedWatermark";

    /** Current number of non-contiguous sender sequence proofs retained by a Reader. */
    public static final String SOURCE_MANAGED_DEDUPE_GAPS = "SourceManagedDedupeGaps";

    /** Cumulative time blocked while a managed Source collector forwards output. */
    public static final String SOURCE_MANAGED_COLLECT_BLOCKED_NANOS =
            "SourceManagedCollectBlockedNs";

    /** Current assignment tracker entry count. */
    public static final String SOURCE_MANAGED_ASSIGNMENT_ENTRIES = "SourceManagedAssignmentEntries";

    /** Current assignment tracker retained bytes. */
    public static final String SOURCE_MANAGED_ASSIGNMENT_BYTES = "SourceManagedAssignmentBytes";

    /** Current age in milliseconds of the oldest uncheckpointed assignment. */
    public static final String SOURCE_MANAGED_ASSIGNMENT_OLDEST_AGE_MILLIS =
            "SourceManagedAssignmentOldestAgeMs";

    /** Assignment tracker state count prefix followed by a bounded state suffix. */
    public static final String SOURCE_MANAGED_ASSIGNMENT_STATE_ENTRIES =
            "SourceManagedAssignmentStateEntries";

    /** Total assignment tracker entries removed after durable ownership reconciliation. */
    public static final String SOURCE_MANAGED_ASSIGNMENT_COMPACTION_TOTAL =
            "SourceManagedAssignmentCompactionTotal";

    /** Cumulative time split requests were deferred by assignment tracker backpressure. */
    public static final String SOURCE_MANAGED_ASSIGNMENT_BACKPRESSURE_NANOS =
            "SourceManagedAssignmentBackpressureNs";

    /** Cumulative time spent reconciling Reader registration and assignment replay. */
    public static final String SOURCE_MANAGED_REGISTRATION_RECONCILIATION_NANOS =
            "SourceManagedRegistrationReconciliationNs";

    /** Current engine-managed coordinator async running count. */
    public static final String SOURCE_MANAGED_ASYNC_RUNNING = "SourceManagedAsyncRunning";

    /** Current engine-managed coordinator async waiting count. */
    public static final String SOURCE_MANAGED_ASYNC_WAITING = "SourceManagedAsyncWaiting";

    /** Current engine-managed coordinator callback queue count. */
    public static final String SOURCE_MANAGED_ASYNC_CALLBACKS = "SourceManagedAsyncCallbacks";

    /** Total engine-managed coordinator async timeout count. */
    public static final String SOURCE_MANAGED_ASYNC_TIMEOUT_TOTAL =
            "SourceManagedAsyncTimeoutTotal";

    /** Total engine-managed coordinator overlap coalescing count. */
    public static final String SOURCE_MANAGED_ASYNC_COALESCED_TOTAL =
            "SourceManagedAsyncCoalescedTotal";

    /** Total engine-managed coordinator overlap skip count. */
    public static final String SOURCE_MANAGED_ASYNC_SKIPPED_TOTAL =
            "SourceManagedAsyncSkippedTotal";

    /** Total stale coordinator callback count rejected by epoch fencing. */
    public static final String SOURCE_MANAGED_ASYNC_STALE_TOTAL = "SourceManagedAsyncStaleTotal";

    /** Cumulative queue wait time for engine-managed coordinator async work. */
    public static final String SOURCE_MANAGED_ASYNC_QUEUE_NANOS = "SourceManagedAsyncQueueNs";

    /** Cumulative worker execution time for engine-managed coordinator async work. */
    public static final String SOURCE_MANAGED_ASYNC_EXECUTION_NANOS =
            "SourceManagedAsyncExecutionNs";

    /** Total nanoseconds spent processing records in Transform chain. */
    public static final String TRANSFORM_PROCESS_NANOS = "TransformProcessNs";

    /** Total records received by Transform chain. */
    public static final String TRANSFORM_RECORDS_IN = "TransformRecordsIn";

    /** Total records emitted by Transform chain. */
    public static final String TRANSFORM_RECORDS_OUT = "TransformRecordsOut";

    /** Total nanoseconds spent writing records in Sink writer.write. */
    public static final String SINK_WRITE_NANOS = "SinkWriteNs";

    /** Total records received by Sink (writer.write call count). */
    public static final String SINK_RECORDS_IN = "SinkRecordsIn";

    /** Total nanoseconds spent in Sink writer.prepareCommit. */
    public static final String SINK_PREPARE_COMMIT_NANOS = "SinkPrepareCommitNs";

    /** Total nanoseconds spent in SinkCommitter.commit. */
    public static final String SINK_COMMIT_NANOS = "SinkCommitNs";

    /** Total nanoseconds spent in SinkCommitter.abort. */
    public static final String SINK_ABORT_NANOS = "SinkAbortNs";

    public static final String FLUSH_SIGNAL_TOTAL = "FlushSignalTotal";
    public static final String FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL = "FlushSignalQueueSuccessTotal";
    public static final String FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL = "FlushSignalQueueFailureTotal";
    public static final String FLUSH_SIGNAL_SINK_SUCCESS_TOTAL = "FlushSignalSinkSuccessTotal";
    public static final String FLUSH_SIGNAL_SINK_FAILURE_TOTAL = "FlushSignalSinkFailureTotal";
    public static final String FLUSH_SIGNAL_QPS = "FlushSignalQPS";
    public static final String FLUSH_SIGNAL_SINK_QPS = "FlushSignalSinkQPS";
}

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

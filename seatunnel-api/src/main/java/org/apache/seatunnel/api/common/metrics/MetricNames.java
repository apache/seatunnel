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

    /** Total row-level sink errors routed to the configured ErrorData sink. */
    public static final String SINK_ERROR_RECORDS_ROUTED = "SinkErrorRecordsRouted";

    /** Total row-level sink errors dropped after logging. */
    public static final String SINK_ERROR_RECORDS_DROPPED = "SinkErrorRecordsDropped";

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

/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SinkWriterSchemaException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.coordinator.LocalSchemaCoordinator;

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class FlinkSinkWriter<CommT, WriterStateT>
        implements CommittingSinkWriter<SeaTunnelRow, CommitWrapper<CommT>>,
                StatefulSinkWriter<SeaTunnelRow, FlinkWriterState<WriterStateT>> {

    private final SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter;
    private final SinkWriter.Context context;
    private final Counter sinkWriteCount;
    private final Counter sinkWriteBytes;
    private final Meter sinkWriterQPS;
    private long checkpointId;
    private MultiTableResourceManager resourceManager;
    private boolean closed = false;
    private boolean isMultiTableSink = false;

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
            WriterInitContext initContext,
            SinkWriter.Context context) {
        this(sinkWriter, initContext, context, 1);
    }

    public FlinkSinkWriter(
            SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
            WriterInitContext initContext,
            SinkWriter.Context context,
            long checkpointId) {
        this.sinkWriter = sinkWriter;
        this.context = context;
        this.checkpointId = checkpointId;
        MetricsContext metricsContext = context.getMetricsContext();
        this.sinkWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.sinkWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
        this.sinkWriterQPS = metricsContext.meter(MetricNames.SINK_WRITE_QPS);

        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
            isMultiTableSink = true;
        }
    }

    @Override
    public void write(
            SeaTunnelRow element, org.apache.flink.api.connector.sink2.SinkWriter.Context context)
            throws IOException, InterruptedException {
        if (element == null) {
            return;
        }

        SeaTunnelRow seaTunnelRow = (SeaTunnelRow) element;
        Map<String, Object> options = seaTunnelRow.getOptions();

        if (options != null && handleControlMessage(options)) {
            return;
        }

        sinkWriter.write(element);
        sinkWriteCount.inc();
        sinkWriteBytes.inc(element.getBytesSize());
        sinkWriterQPS.markEvent();
    }

    private boolean handleControlMessage(Map<String, Object> options) throws IOException {
        if (options.containsKey("schema_change_ack")) {
            log.debug("FlinkSinkWriter received schema change ack - filtering out control message");
            return true;
        }

        if (options.containsKey("schema_change_event")) {
            handleSchemaChangeEvent(
                    (SchemaChangeEvent) options.get("schema_change_event"), options);
            return true;
        }

        return false;
    }

    private void handleSchemaChangeEvent(
            SchemaChangeEvent schemaChangeEvent, Map<String, Object> options) throws IOException {
        log.info(
                "FlinkSinkWriter applying SchemaChangeEvent for table: {}",
                schemaChangeEvent.tableIdentifier());

        sinkWriter.prepareCommit();
        if (!(sinkWriter instanceof SupportSchemaEvolutionSinkWriter)) {
            log.warn(
                    "Sink writer {} does not support schema evolution, ignoring SchemaChangeEvent for table: {}",
                    sinkWriter.getClass().getSimpleName(),
                    schemaChangeEvent.tableIdentifier());
            return;
        }

        Long subtaskIdObj = (Long) options.get("schema_subtask_id");
        int subtaskId = subtaskIdObj != null ? subtaskIdObj.intValue() : -1;
        long epoch = schemaChangeEvent.getCreatedTime();
        boolean success = false;

        try {
            ((SupportSchemaEvolutionSinkWriter) sinkWriter).applySchemaChange(schemaChangeEvent);
            log.info(
                    "FlinkSinkWriter successfully applied SchemaChangeEvent for table: {}",
                    schemaChangeEvent.tableIdentifier());
            success = true;
        } catch (Exception e) {
            log.error(
                    "Failed to apply schema change for table: {}",
                    schemaChangeEvent.tableIdentifier(),
                    e);
        } finally {
            sendSchemaChangeAck(schemaChangeEvent, epoch, subtaskId, success);
        }

        if (!success) {
            throw new SinkWriterSchemaException(
                    SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                    "Failed to apply schema change in Flink sink writer",
                    schemaChangeEvent.tableIdentifier(),
                    schemaChangeEvent.getJobId(),
                    null);
        }
    }

    private void sendSchemaChangeAck(
            SchemaChangeEvent schemaChangeEvent, long epoch, int subtaskId, boolean success) {
        if (subtaskId < 0) {
            log.warn(
                    "FlinkSinkWriter cannot send ack: subtask ID not found in schema change event options");
            return;
        }

        try {
            String jobId = schemaChangeEvent.getJobId();
            if (jobId == null || jobId.trim().isEmpty()) {
                jobId = "unknown-job";
                log.warn("SchemaChangeEvent has no jobId, using default: {}", jobId);
            }

            LocalSchemaCoordinator coordinator = LocalSchemaCoordinator.getInstance(jobId);
            coordinator.notifySchemaChangeApplied(
                    schemaChangeEvent.tableIdentifier(), epoch, subtaskId, success);
            log.info(
                    "FlinkSinkWriter sent schema change ack to coordinator for table {} (epoch {}), subtask {}, success: {}",
                    schemaChangeEvent.tableIdentifier(),
                    epoch,
                    subtaskId,
                    success);
        } catch (Exception e) {
            log.error(
                    "Failed to send schema change ack to coordinator for table {} (epoch {})",
                    schemaChangeEvent.tableIdentifier(),
                    epoch,
                    e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (closed) {
            return;
        }
    }

    @Override
    public Collection<CommitWrapper<CommT>> prepareCommit()
            throws IOException, InterruptedException {
        if (closed) {
            return new ArrayList<>();
        }

        try {
            Optional<CommT> commitInfo = sinkWriter.prepareCommit(this.checkpointId);

            List<CommitWrapper<CommT>> wrappedCommits = new ArrayList<>();
            if (commitInfo.isPresent()) {
                wrappedCommits.add(new CommitWrapper<>(commitInfo.get()));
            }
            return wrappedCommits;
        } catch (Exception e) {
            throw new IOException("Failed to prepare commit for sink writer", e);
        }
    }

    @Override
    public List<FlinkWriterState<WriterStateT>> snapshotState(long checkpointId)
            throws IOException {
        try {
            List<WriterStateT> states = sinkWriter.snapshotState(checkpointId);
            List<FlinkWriterState<WriterStateT>> wrappedStates = new ArrayList<>();
            if (states != null) {
                for (WriterStateT state : states) {
                    wrappedStates.add(new FlinkWriterState<>(checkpointId, state));
                }
            }

            log.debug(
                    "Snapshotted {} states for checkpointId: {}",
                    wrappedStates.size(),
                    checkpointId);

            // Update internal checkpoint ID for next checkpoint (similar to flink-common)
            // This is critical for maintaining transaction boundaries in schema evolution scenarios
            long previousCheckpointId = this.checkpointId;
            this.checkpointId = checkpointId + 1;

            log.debug(
                    "Updated internal checkpointId from {} to {} after snapshot",
                    previousCheckpointId,
                    this.checkpointId);

            return wrappedStates;
        } catch (Exception e) {
            log.error("Error during state snapshot for checkpointId: {}", checkpointId, e);
            throw new IOException("Failed to snapshot writer state", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        try {
            // Perform final flush before closing to ensure all data is committed
            log.debug("Performing final flush before closing sink writer");
            flush(true);
        } catch (Exception e) {
            log.warn("Error during final flush before close", e);
            // Continue with close even if flush fails
        }

        try {
            sinkWriter.close();
            context.getEventListener().onEvent(new WriterCloseEvent());
        } catch (Exception e) {
            log.error("Error closing sink writer: " + e.getMessage(), e);
        } finally {
            closed = true;
        }

        // Close resource manager
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}

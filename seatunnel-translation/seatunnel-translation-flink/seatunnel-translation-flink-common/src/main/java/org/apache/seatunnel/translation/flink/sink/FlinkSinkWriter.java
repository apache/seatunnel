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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SinkWriterSchemaException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.coordinator.LocalSchemaCoordinator;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The sink writer implementation of {@link SinkWriter}, which is created by {@link
 * Sink#createWriter}
 *
 * @param <InputT> The generic type of input data
 * @param <CommT> The generic type of commit message
 * @param <WriterStateT> The generic type of writer state
 */
@Slf4j
public class FlinkSinkWriter<InputT, CommT, WriterStateT>
        implements SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> {

    private final org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
            sinkWriter;

    private final org.apache.seatunnel.api.sink.SinkWriter.Context context;

    private final Counter sinkWriteCount;

    private final Counter sinkWriteBytes;

    private final Meter sinkWriterQPS;

    private long checkpointId;

    private MultiTableResourceManager resourceManager;

    /**
     * Cached writer states produced together with {@link #prepareCommit(boolean)}.
     *
     * <p>Flink 1.13+ calls {@code prepareCommit(..)} in {@code prepareSnapshotPreBarrier} and
     * {@code snapshotState(..)} afterwards. To guarantee that SeaTunnel {@link
     * org.apache.seatunnel.api.sink.SinkWriter} always sees {@code prepareCommit(checkpointId)}
     * followed immediately by {@code snapshotState(checkpointId)} without any further writes to the
     * same transaction, we invoke {@code snapshotState(checkpointId)} inside {@link
     * #prepareCommit(boolean)} and cache the result here. The subsequent Flink {@code
     * snapshotState(..)} call simply consumes this cached state.
     */
    private List<FlinkWriterState<WriterStateT>> pendingStates;

    FlinkSinkWriter(
            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
            long checkpointId,
            org.apache.seatunnel.api.sink.SinkWriter.Context context) {
        this.context = context;
        this.sinkWriter = sinkWriter;
        this.checkpointId = checkpointId;
        MetricsContext metricsContext = context.getMetricsContext();
        this.sinkWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.sinkWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
        this.sinkWriterQPS = metricsContext.meter(MetricNames.SINK_WRITE_QPS);
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public void write(InputT element, SinkWriter.Context context) throws IOException {
        if (element == null) {
            return;
        }

        SeaTunnelRow seaTunnelRow = (SeaTunnelRow) element;
        Map<String, Object> options = seaTunnelRow.getOptions();

        if (options != null && handleControlMessage(options)) {
            return;
        }

        sinkWriter.write(seaTunnelRow);
        sinkWriteCount.inc();
        sinkWriteBytes.inc(seaTunnelRow.getBytesSize());
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
    public List<CommitWrapper<CommT>> prepareCommit(boolean flush) throws IOException {
        // 1. Let SeaTunnel SinkWriter prepare the commit for the current checkpointId
        Optional<CommT> commTOptional = sinkWriter.prepareCommit(checkpointId);

        // 2. Immediately snapshot state for the same checkpointId, so from SeaTunnel's
        //    perspective prepareCommit(checkpointId) and snapshotState(checkpointId) are
        //    executed back-to-back with no further writes to the same transaction.
        List<FlinkWriterState<WriterStateT>> states =
                sinkWriter.snapshotState(this.checkpointId).stream()
                        .map(state -> new FlinkWriterState<>(this.checkpointId, state))
                        .collect(Collectors.toList());
        this.pendingStates = states;

        // 3. Advance internal checkpointId for the next round.
        this.checkpointId++;

        // 4. Wrap commit info as before.
        return commTOptional
                .map(CommitWrapper::new)
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());
    }

    @Override
    public List<FlinkWriterState<WriterStateT>> snapshotState() throws IOException {
        // If we have already snapshotted state in prepareCommit for this checkpoint,
        // just return the cached value to Flink and avoid calling the underlying
        // SeaTunnel SinkWriter.snapshotState(..) a second time.
        if (pendingStates != null) {
            List<FlinkWriterState<WriterStateT>> states = pendingStates;
            pendingStates = null;
            return states;
        }

        // Fallback: in some edge cases (e.g., sinks without 2PC) Flink might call snapshotState
        // without a preceding prepareCommit. Preserve the original behaviour then.
        List<FlinkWriterState<WriterStateT>> states =
                sinkWriter.snapshotState(this.checkpointId).stream()
                        .map(state -> new FlinkWriterState<>(this.checkpointId, state))
                        .collect(Collectors.toList());
        this.checkpointId++;
        return states;
    }

    @Override
    public void close() throws Exception {
        sinkWriter.close();
        context.getEventListener().onEvent(new WriterCloseEvent());
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}

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
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SinkWriter.Context;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.event.JobEventListener;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.ConnectorMetricsCalcContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SinkWriterContext;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupAddressOperation;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkPrepareCommitOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkRegisterOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;
import org.apache.seatunnel.engine.server.trace.StainTraceUtils;

import com.hazelcast.cluster.Address;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;
import static org.apache.seatunnel.engine.server.task.AbstractTask.serializeStates;

/** Drives the sink writer lifecycle, checkpointing, and final stain trace event emission. */
@Slf4j
public class SinkFlowLifeCycle<T, CommitInfoT extends Serializable, AggregatedCommitInfoT, StateT>
        extends ActionFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>>, InternalCheckpointListener {

    private final SinkAction<T, StateT, CommitInfoT, AggregatedCommitInfoT> sinkAction;
    private SinkWriter<T, CommitInfoT, StateT> writer;
    private Context writerContext;

    private transient Optional<Serializer<CommitInfoT>> commitInfoSerializer;
    private transient Optional<Serializer<StateT>> writerStateSerializer;

    private final int indexID;

    private final TaskLocation taskLocation;

    private Address committerTaskAddress;

    private final TaskLocation committerTaskLocation;

    private Optional<SinkCommitter<CommitInfoT>> committer;

    private Optional<CommitInfoT> lastCommitInfo;

    private final MetricsContext metricsContext;

    private final ConnectorMetricsCalcContext connectorMetricsCalcContext;

    private final boolean containAggCommitter;

    private final EventListener eventListener;

    /** Mapping relationship between upstream TablePath and downstream TablePath. */
    private final Map<TablePath, TablePath> tablesMaps = new HashMap<>();

    private final Counter stainTraceEventsReportedTotal;
    private final Counter stainTraceInvalidPayloadTotal;
    private volatile Counter stainTraceEntriesTruncatedTotal;
    private volatile int stainTraceMaxEntriesPerTrace = -1;

    public SinkFlowLifeCycle(
            SinkAction<T, StateT, CommitInfoT, AggregatedCommitInfoT> sinkAction,
            TaskLocation taskLocation,
            int indexID,
            SeaTunnelTask runningTask,
            TaskLocation committerTaskLocation,
            boolean containAggCommitter,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext) {
        super(sinkAction, runningTask, completableFuture);
        this.sinkAction = sinkAction;
        this.indexID = indexID;
        this.taskLocation = taskLocation;
        this.committerTaskLocation = committerTaskLocation;
        this.containAggCommitter = containAggCommitter;
        this.metricsContext = metricsContext;
        this.eventListener = new JobEventListener(taskLocation, runningTask.getExecutionContext());
        this.stainTraceEventsReportedTotal =
                metricsContext.counter(StainTraceConstants.METRIC_EVENTS_REPORTED_TOTAL);
        List<TablePath> sinkTables = new ArrayList<>();
        boolean isMulti = sinkAction.getSink() instanceof MultiTableSink;
        if (isMulti) {
            sinkTables = ((MultiTableSink) sinkAction.getSink()).getSinkTables();
            tablesMaps.putAll(((MultiTableSink) sinkAction.getSink()).getSinkTableMapping());
        } else {
            Optional<CatalogTable> catalogTable = sinkAction.getSink().getWriteCatalogTable();
            if (catalogTable.isPresent()) {
                sinkTables.add(catalogTable.get().getTablePath());
            } else {
                sinkTables.add(TablePath.DEFAULT);
            }
        }
        this.connectorMetricsCalcContext =
                new ConnectorMetricsCalcContext(
                        metricsContext, PluginType.SINK, isMulti, sinkTables);
        this.stainTraceInvalidPayloadTotal =
                metricsContext.counter(StainTraceConstants.METRIC_INVALID_PAYLOAD_TOTAL);
    }

    @Override
    public void init() throws Exception {
        this.commitInfoSerializer = sinkAction.getSink().getCommitInfoSerializer();
        this.writerStateSerializer = sinkAction.getSink().getWriterStateSerializer();
        this.committer = sinkAction.getSink().createCommitter();
        this.lastCommitInfo = Optional.empty();
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (containAggCommitter) {
            committerTaskAddress = getCommitterTaskAddress();
        }
        registerCommitter();
    }

    private Address getCommitterTaskAddress() throws ExecutionException, InterruptedException {
        return (Address)
                runningTask
                        .getExecutionContext()
                        .sendToMaster(new GetTaskGroupAddressOperation(committerTaskLocation))
                        .get();
    }

    @Override
    public void close() throws IOException {
        super.close();
        writer.close();
        writerContext.getEventListener().onEvent(new WriterCloseEvent());
    }

    private void registerCommitter() {
        if (containAggCommitter) {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SinkRegisterOperation(taskLocation, committerTaskLocation),
                            committerTaskAddress)
                    .join();
        }
    }

    /** Handles barriers for checkpointing and rows for sink writes plus trace event reporting. */
    @Override
    public void received(Record<?> record) {
        try {
            if (record.getData() instanceof Barrier) {
                long startTime = System.currentTimeMillis();

                Barrier barrier = (Barrier) record.getData();
                connectorMetricsCalcContext.sealCheckpointMetrics(barrier.getId());
                if (barrier.prepareClose(this.taskLocation)) {
                    prepareClose = true;
                }
                if (barrier.snapshot()) {
                    try {
                        lastCommitInfo = writer.prepareCommit(barrier.getId());
                    } catch (Exception e) {
                        writer.abortPrepare();
                        throw e;
                    }
                    List<StateT> states = writer.snapshotState(barrier.getId());
                    if (!writerStateSerializer.isPresent()) {
                        runningTask.addState(
                                barrier, ActionStateKey.of(sinkAction), Collections.emptyList());
                    } else {
                        runningTask.addState(
                                barrier,
                                ActionStateKey.of(sinkAction),
                                serializeStates(writerStateSerializer.get(), states));
                    }
                    if (containAggCommitter) {
                        CommitInfoT commitInfoT = null;
                        if (lastCommitInfo.isPresent()) {
                            commitInfoT = lastCommitInfo.get();
                        }
                        runningTask
                                .getExecutionContext()
                                .sendToMember(
                                        new SinkPrepareCommitOperation<CommitInfoT>(
                                                barrier,
                                                committerTaskLocation,
                                                commitInfoSerializer.isPresent()
                                                        ? commitInfoSerializer
                                                                .get()
                                                                .serialize(commitInfoT)
                                                        : null),
                                        committerTaskAddress)
                                .join();
                    }
                } else {
                    if (containAggCommitter) {
                        runningTask
                                .getExecutionContext()
                                .sendToMember(
                                        new BarrierFlowOperation(barrier, committerTaskLocation),
                                        committerTaskAddress)
                                .join();
                    }
                }
                runningTask.ack(barrier);

                log.debug(
                        "trigger barrier [{}] finished, cost {}ms. taskLocation [{}]",
                        barrier.getId(),
                        System.currentTimeMillis() - startTime,
                        taskLocation);
            } else if (record.getData() instanceof SchemaChangeEvent) {
                if (prepareClose) {
                    return;
                }
                SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
                if (writer instanceof SupportSchemaEvolutionSinkWriter) {
                    ((SupportSchemaEvolutionSinkWriter) writer).applySchemaChange(event);
                } else {
                    // todo remove deprecated method
                    writer.applySchemaChange(event);
                }
            } else {
                if (prepareClose) {
                    return;
                }
                String tableId;
                writer.write((T) record.getData());
                if (record.getData() instanceof SeaTunnelRow) {
                    SeaTunnelRow row = (SeaTunnelRow) record.getData();
                    if (this.sinkAction.getSink() instanceof MultiTableSink) {
                        if (row.getTableId() == null || row.getTableId().isEmpty()) {
                            tableId = row.getTableId();
                        } else {

                            TablePath tablePath = tablesMaps.get(TablePath.of(row.getTableId()));
                            tableId =
                                    tablePath != null
                                            ? tablePath.getFullName()
                                            : TablePath.DEFAULT.getFullName();
                        }

                    } else {
                        Optional<CatalogTable> writeCatalogTable =
                                this.sinkAction.getSink().getWriteCatalogTable();
                        tableId =
                                writeCatalogTable
                                        .map(
                                                catalogTable ->
                                                        catalogTable.getTablePath().getFullName())
                                        .orElseGet(TablePath.DEFAULT::getFullName);
                    }

                    connectorMetricsCalcContext.updateMetrics(record.getData(), tableId);

                    if (StainTraceUtils.hasPayload(row)) {
                        long nowMs = System.currentTimeMillis();
                        StainTraceUtils.appendIfPresent(
                                row,
                                StainTraceStage.SINK_WRITE_DONE,
                                runningTask.getTaskID(),
                                nowMs,
                                getStainTraceMaxEntriesPerTrace(),
                                getStainTraceEntriesTruncatedTotal());
                        byte[] payload = StainTraceUtils.getPayloadOrNull(row);
                        if (payload != null) {
                            try {
                                long traceId = StainTracePayload.readTraceId(payload);
                                eventListener.onEvent(
                                        new StainTraceEvent(
                                                traceId,
                                                payload,
                                                taskLocation.getTaskID(),
                                                tableId));
                                stainTraceEventsReportedTotal.inc();
                            } catch (Exception e) {
                                stainTraceInvalidPayloadTotal.inc();
                                log.debug("Failed to report stain trace event", e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (committer.isPresent() && lastCommitInfo.isPresent()) {
            committer.get().commit(Collections.singletonList(lastCommitInfo.get()));
        }
        connectorMetricsCalcContext.commitPendingMetrics(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (committer.isPresent() && lastCommitInfo.isPresent()) {
            committer.get().abort(Collections.singletonList(lastCommitInfo.get()));
        }
        connectorMetricsCalcContext.abortPendingMetrics(checkpointId);
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        List<StateT> states = new ArrayList<>();
        if (writerStateSerializer.isPresent()) {
            states =
                    actionStateList.stream()
                            .map(ActionSubtaskState::getState)
                            .flatMap(Collection::stream)
                            .filter(Objects::nonNull)
                            .map(
                                    bytes ->
                                            sneaky(
                                                    () ->
                                                            writerStateSerializer
                                                                    .get()
                                                                    .deserialize(bytes)))
                            .collect(Collectors.toList());
        }
        this.writerContext =
                new SinkWriterContext(
                        sinkAction.getParallelism(), indexID, metricsContext, eventListener);
        if (states.isEmpty()) {
            this.writer = sinkAction.getSink().createWriter(writerContext);
        } else {
            this.writer = sinkAction.getSink().restoreWriter(writerContext, states);
        }
    }

    private Counter getStainTraceEntriesTruncatedTotal() {
        if (stainTraceEntriesTruncatedTotal == null) {
            synchronized (this) {
                if (stainTraceEntriesTruncatedTotal == null) {
                    stainTraceEntriesTruncatedTotal =
                            runningTask
                                    .getMetricsContext()
                                    .counter(StainTraceConstants.METRIC_ENTRIES_TRUNCATED_TOTAL);
                }
            }
        }
        return stainTraceEntriesTruncatedTotal;
    }

    private int getStainTraceMaxEntriesPerTrace() {
        if (stainTraceMaxEntriesPerTrace < 0) {
            synchronized (this) {
                if (stainTraceMaxEntriesPerTrace < 0) {
                    stainTraceMaxEntriesPerTrace =
                            runningTask
                                    .getExecutionContext()
                                    .getTaskExecutionService()
                                    .getSeaTunnelConfig()
                                    .getEngineConfig()
                                    .getStainTraceMaxEntriesPerTrace();
                }
            }
        }
        return stainTraceMaxEntriesPerTrace;
    }
}

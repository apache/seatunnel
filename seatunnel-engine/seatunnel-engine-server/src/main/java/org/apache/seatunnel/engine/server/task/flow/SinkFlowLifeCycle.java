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
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.signal.Signal;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SinkWriter.Context;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
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
import org.apache.seatunnel.engine.server.task.error.DefaultErrorSinkWriter;
import org.apache.seatunnel.engine.server.task.error.DefaultRowErrorClassifier;
import org.apache.seatunnel.engine.server.task.error.EngineMultiTableRowErrorHandler;
import org.apache.seatunnel.engine.server.task.error.EngineRowErrorCollector;
import org.apache.seatunnel.engine.server.task.error.ErrorHandler;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerConfigUtil;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerConfigUtil.StageType;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerMode;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlingSinkWriter;
import org.apache.seatunnel.engine.server.task.error.ErrorSinkConfig;
import org.apache.seatunnel.engine.server.task.error.ErrorSinkRowWriter;
import org.apache.seatunnel.engine.server.task.error.RowErrorClassifier;
import org.apache.seatunnel.engine.server.task.error.StageErrorConfig;
import org.apache.seatunnel.engine.server.task.error.SynchronizedErrorSinkRowWriter;
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

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_ABORT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMIT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_ERROR_RECORDS_DROPPED;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_ERROR_RECORDS_ROUTED;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_PREPARE_COMMIT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_NANOS;
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

    private final boolean containAggCommitter;

    private final EventListener eventListener;

    /** Mapping relationship between upstream TablePath and downstream TablePath. */
    private final Map<TablePath, TablePath> tablesMaps = new HashMap<>();

    private final MetricsContext metricsContext;

    private final ConnectorMetricsCalcContext connectorMetricsCalcContext;

    private final Counter sinkWriteNs;
    private final Counter sinkRecordsIn;
    private final Counter sinkErrorRecordsRouted;
    private final Counter sinkErrorRecordsDropped;
    private final Counter sinkPrepareCommitNs;
    private final Counter sinkCommitNs;
    private final Counter sinkAbortNs;

    private transient StageErrorConfig stageErrorConfig;
    private transient ErrorHandler<T> stageErrorHandler;
    private transient RowErrorClassifier<T> stageRowErrorClassifier;
    private transient org.apache.seatunnel.api.common.error.RowErrorCollector
            stageRowErrorCollector;

    private final Counter stainTraceEventsReportedTotal;
    private final Counter stainTraceInvalidPayloadTotal;
    private final Counter flushSignalSinkSuccessTotal;
    private final Counter flushSignalSinkFailureTotal;
    private final Meter flushSignalSinkQPS;
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
        long sinkId = sinkAction.getId();
        this.sinkWriteNs = metricsContext.counter(SINK_WRITE_NANOS + "#" + sinkId);
        this.sinkRecordsIn = metricsContext.counter(SINK_RECORDS_IN + "#" + sinkId);
        this.sinkErrorRecordsRouted =
                metricsContext.counter(SINK_ERROR_RECORDS_ROUTED + "#" + sinkId);
        this.sinkErrorRecordsDropped =
                metricsContext.counter(SINK_ERROR_RECORDS_DROPPED + "#" + sinkId);
        this.sinkPrepareCommitNs = metricsContext.counter(SINK_PREPARE_COMMIT_NANOS + "#" + sinkId);
        this.sinkCommitNs = metricsContext.counter(SINK_COMMIT_NANOS + "#" + sinkId);
        this.sinkAbortNs = metricsContext.counter(SINK_ABORT_NANOS + "#" + sinkId);
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
        this.flushSignalSinkSuccessTotal =
                metricsContext.counter(MetricNames.FLUSH_SIGNAL_SINK_SUCCESS_TOTAL);
        this.flushSignalSinkFailureTotal =
                metricsContext.counter(MetricNames.FLUSH_SIGNAL_SINK_FAILURE_TOTAL);
        this.flushSignalSinkQPS = metricsContext.meter(MetricNames.FLUSH_SIGNAL_SINK_QPS);
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
                Barrier barrier = (Barrier) record.getData();
                processCheckpointBarrier(barrier);
            } else if (record.getData() instanceof SchemaChangeEvent) {
                if (prepareClose) {
                    return;
                }
                SchemaChangeEvent event = (SchemaChangeEvent) record.getData();
                processSchemaChangeEvent(event);
            } else if (record.getData() instanceof Signal) {
                if (prepareClose) {
                    return;
                }
                Signal signal = (Signal) record.getData();
                processSignal(signal);
            } else {
                if (prepareClose) {
                    return;
                }
                processDataRecord(record);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (committer.isPresent() && lastCommitInfo.isPresent()) {
            boolean metricsEnabled = runningTask != null && runningTask.isObservabilityEnabled();
            long commitStartNs = metricsEnabled ? System.nanoTime() : 0L;
            committer.get().commit(Collections.singletonList(lastCommitInfo.get()));
            if (metricsEnabled) {
                sinkCommitNs.inc(System.nanoTime() - commitStartNs);
            }
        }
        connectorMetricsCalcContext.commitPendingMetrics(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (committer.isPresent() && lastCommitInfo.isPresent()) {
            boolean metricsEnabled = runningTask != null && runningTask.isObservabilityEnabled();
            long abortStartNs = metricsEnabled ? System.nanoTime() : 0L;
            committer.get().abort(Collections.singletonList(lastCommitInfo.get()));
            if (metricsEnabled) {
                sinkAbortNs.inc(System.nanoTime() - abortStartNs);
            }
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
        initRowErrorCollectorIfNeed();
        this.writerContext =
                new SinkWriterContext(
                        sinkAction.getParallelism(),
                        indexID,
                        metricsContext,
                        eventListener,
                        stageRowErrorCollector);
        if (states.isEmpty()) {
            this.writer = sinkAction.getSink().createWriter(writerContext);
        } else {
            this.writer = sinkAction.getSink().restoreWriter(writerContext, states);
        }
        wrapWriterIfNeed();
    }

    @SuppressWarnings("unchecked")
    private void initRowErrorCollectorIfNeed() {
        if (!(runningTask instanceof SeaTunnelTask)) {
            return;
        }
        SeaTunnelTask seaTunnelTask = (SeaTunnelTask) runningTask;
        StageErrorConfig stageConfig =
                ErrorHandlerConfigUtil.buildStageConfig(
                        seaTunnelTask.getEnvOptions(),
                        StageType.SINK,
                        getJobIdOrDefault(seaTunnelTask));
        this.stageErrorConfig = stageConfig;
        if (stageConfig.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }

        ErrorSinkRowWriter<T> errorSinkWriter = createErrorSinkWriter(seaTunnelTask, stageConfig);
        ErrorHandler<T> handler = new ErrorHandler<>(stageConfig, errorSinkWriter);
        RowErrorClassifier<T> classifier = new DefaultRowErrorClassifier<>();

        this.stageErrorHandler = handler;
        this.stageRowErrorClassifier = classifier;

        // Expose collector for row-level errors during flush/commit/close.
        String pluginName = sinkAction.getSink().getPluginName();
        ErrorHandler<SeaTunnelRow> rowHandler = (ErrorHandler<SeaTunnelRow>) handler;
        this.stageRowErrorCollector = new EngineRowErrorCollector(rowHandler, pluginName);
    }

    private void wrapWriterIfNeed() {
        if (!(runningTask instanceof SeaTunnelTask)) {
            return;
        }
        SeaTunnelTask seaTunnelTask = (SeaTunnelTask) runningTask;
        StageErrorConfig stageConfig = stageErrorConfig;
        if (stageConfig == null) {
            stageConfig =
                    ErrorHandlerConfigUtil.buildStageConfig(
                            seaTunnelTask.getEnvOptions(),
                            StageType.SINK,
                            getJobIdOrDefault(seaTunnelTask));
            stageErrorConfig = stageConfig;
        }
        if (stageConfig.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }

        ErrorHandler<T> handler = stageErrorHandler;
        RowErrorClassifier<T> classifier = stageRowErrorClassifier;
        if (handler == null || classifier == null) {
            ErrorSinkRowWriter<T> errorSinkWriter =
                    createErrorSinkWriter(seaTunnelTask, stageConfig);
            handler = new ErrorHandler<>(stageConfig, errorSinkWriter);
            classifier = new DefaultRowErrorClassifier<>();
            stageErrorHandler = handler;
            stageRowErrorClassifier = classifier;
        }
        String pluginName = sinkAction.getSink().getPluginName();

        if (this.writer instanceof MultiTableSinkWriter) {
            @SuppressWarnings("unchecked")
            MultiTableSinkWriter multiTableSinkWriter = (MultiTableSinkWriter) this.writer;
            @SuppressWarnings("unchecked")
            ErrorHandler<SeaTunnelRow> rowHandler = (ErrorHandler<SeaTunnelRow>) handler;
            @SuppressWarnings("unchecked")
            RowErrorClassifier<SeaTunnelRow> rowClassifier =
                    (RowErrorClassifier<SeaTunnelRow>) classifier;
            multiTableSinkWriter.setRowErrorHandler(
                    new EngineMultiTableRowErrorHandler(rowHandler, rowClassifier, pluginName));
        }

        ErrorHandlingSinkWriter<T, CommitInfoT, StateT> errorHandlingWriter =
                new ErrorHandlingSinkWriter<>(this.writer, handler, classifier, pluginName);
        errorHandlingWriter.registerFlushAction(writerContext);
        this.writer = errorHandlingWriter;
    }

    private static long getJobIdOrDefault(SeaTunnelTask seaTunnelTask) {
        TaskLocation taskLocation = seaTunnelTask.getTaskLocation();
        return taskLocation == null ? -1L : taskLocation.getJobId();
    }

    @SuppressWarnings("unchecked")
    private ErrorSinkRowWriter<T> createErrorSinkWriter(
            SeaTunnelTask seaTunnelTask, StageErrorConfig stageConfig) {
        if (stageConfig.getMode() != ErrorHandlerMode.ROUTE) {
            return null;
        }
        ErrorSinkConfig sinkConfig = stageConfig.getSink();
        if (sinkConfig == null || !sinkConfig.isConfigured()) {
            return null;
        }
        return (ErrorSinkRowWriter<T>)
                new SynchronizedErrorSinkRowWriter<>(
                        new DefaultErrorSinkWriter<>(
                                stageConfig,
                                sinkConfig,
                                seaTunnelTask.getTaskLocation().getJobId(),
                                seaTunnelTask.getTaskLocation().getTaskIndex(),
                                seaTunnelTask.getExecutionContext().getClassLoaderService()));
    }

    private void processDataRecord(Record<?> record) throws IOException {
        boolean metricsEnabled = runningTask != null && runningTask.isObservabilityEnabled();
        String tableId;
        long writeStartNs = metricsEnabled ? System.nanoTime() : 0L;
        ErrorHandlingSinkWriter.WriteOutcome writeOutcome;
        if (writer instanceof ErrorHandlingSinkWriter) {
            writeOutcome =
                    ((ErrorHandlingSinkWriter<T, CommitInfoT, StateT>) writer)
                            .writeWithOutcome((T) record.getData());
        } else {
            writer.write((T) record.getData());
            writeOutcome = ErrorHandlingSinkWriter.WriteOutcome.WRITTEN;
        }
        if (metricsEnabled) {
            sinkWriteNs.inc(System.nanoTime() - writeStartNs);
            sinkRecordsIn.inc();
        }
        if (writeOutcome == ErrorHandlingSinkWriter.WriteOutcome.ROUTED_TO_ERROR_SINK) {
            sinkErrorRecordsRouted.inc();
        } else if (writeOutcome == ErrorHandlingSinkWriter.WriteOutcome.DROPPED) {
            sinkErrorRecordsDropped.inc();
        }
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
                                .map(catalogTable -> catalogTable.getTablePath().getFullName())
                                .orElseGet(TablePath.DEFAULT::getFullName);
            }

            if (writeOutcome == ErrorHandlingSinkWriter.WriteOutcome.WRITTEN) {
                connectorMetricsCalcContext.updateMetrics(record.getData(), tableId);
            }

            if (StainTraceUtils.hasPayload(row)) {
                long nowMs = System.currentTimeMillis();
                StainTraceStage traceStage;
                switch (writeOutcome) {
                    case ROUTED_TO_ERROR_SINK:
                        traceStage = StainTraceStage.SINK_ERROR_ROUTED;
                        break;
                    case DROPPED:
                        traceStage = StainTraceStage.SINK_ERROR_DROPPED;
                        break;
                    case WRITTEN:
                    default:
                        traceStage = StainTraceStage.SINK_WRITE_DONE;
                        break;
                }
                StainTraceUtils.appendIfPresent(
                        row,
                        traceStage,
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
                                        traceId, payload, taskLocation.getTaskID(), tableId));
                        stainTraceEventsReportedTotal.inc();
                    } catch (Exception e) {
                        stainTraceInvalidPayloadTotal.inc();
                        log.debug("Failed to report stain trace event", e);
                    }
                }
            }
        }
    }

    private void processSignal(Signal signal) throws Exception {
        if (signal instanceof FlushSignal && writerContext.getFlushAction() != null) {
            try {
                writerContext.getFlushAction().run();
                flushSignalSinkSuccessTotal.inc();
                flushSignalSinkQPS.markEvent();
            } catch (Exception e) {
                flushSignalSinkFailureTotal.inc();
                throw e;
            }
        }
    }

    private void processCheckpointBarrier(Barrier barrier) throws IOException {
        boolean metricsEnabled = runningTask != null && runningTask.isObservabilityEnabled();
        long startTime = System.currentTimeMillis();
        connectorMetricsCalcContext.sealCheckpointMetrics(barrier.getId());
        if (barrier.prepareClose(this.taskLocation)) {
            prepareClose = true;
        }
        if (barrier.snapshot()) {
            try {
                long prepareStartNs = metricsEnabled ? System.nanoTime() : 0L;
                lastCommitInfo = writer.prepareCommit(barrier.getId());
                if (metricsEnabled) {
                    sinkPrepareCommitNs.inc(System.nanoTime() - prepareStartNs);
                }
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
                                                ? commitInfoSerializer.get().serialize(commitInfoT)
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
    }

    private void processSchemaChangeEvent(SchemaChangeEvent event) throws IOException {
        if (writer instanceof SupportSchemaEvolutionSinkWriter) {
            ((SupportSchemaEvolutionSinkWriter) writer).applySchemaChange(event);
        } else {
            // todo remove deprecated method
            writer.applySchemaChange(event);
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

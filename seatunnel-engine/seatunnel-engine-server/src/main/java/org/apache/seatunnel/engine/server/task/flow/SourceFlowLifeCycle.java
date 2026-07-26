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
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.event.ReaderCloseEvent;
import org.apache.seatunnel.api.source.event.ReaderOpenEvent;
import org.apache.seatunnel.api.source.managed.ManagedSourceReader;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;
import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeSelection;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.event.JobEventListener;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SourceReaderContext;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupAddressOperation;
import org.apache.seatunnel.engine.server.task.operation.source.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.RestoredSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceNoMoreElementOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceReaderEventOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceRegisterOperation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.source.ManagedReaderCheckpointState;
import org.apache.seatunnel.engine.server.task.source.ManagedReaderCheckpointStateSerializer;
import org.apache.seatunnel.engine.server.task.source.ManagedSourceReaderRuntime;
import org.apache.seatunnel.engine.server.task.source.SourceCommandAdmissionAck;
import org.apache.seatunnel.engine.server.task.source.SourceCommandAdmissionStatus;
import org.apache.seatunnel.engine.server.task.source.SourceCommandEnvelope;

import com.hazelcast.cluster.Address;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_IDLE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READ_NANOS;
import static org.apache.seatunnel.engine.server.task.AbstractTask.serializeStates;

/**
 * Runtime lifecycle bridge between the Zeta engine and a connector's {@link SourceReader}.
 *
 * <p>This class manages the full lifecycle of a source reader within a Zeta worker task, including:
 *
 * <ul>
 *   <li>Creating and opening the {@link SourceReader} from the {@link SourceAction}
 *   <li>Registering with the remote {@link org.apache.seatunnel.api.source.SourceSplitEnumerator}
 *       and requesting splits
 *   <li>Running the core read loop via {@link #collect()}
 *   <li>Handling checkpoint barriers with proper checkpoint-lock synchronization
 *   <li>Coordinating schema-change signals (before/after checkpoint phases)
 * </ul>
 *
 * @param <T> the type of records produced by the source
 * @param <SplitT> the type of source splits
 */
@Slf4j
public class SourceFlowLifeCycle<T, SplitT extends SourceSplit> extends ActionFlowLifeCycle
        implements InternalCheckpointListener {

    private static final long SCHEMA_CHANGE_SLEEP_MS = 200L;
    private static final long SCHEMA_CHANGE_SLEEP_NS =
            TimeUnit.MILLISECONDS.toNanos(SCHEMA_CHANGE_SLEEP_MS);
    private static final long IDLE_SLEEP_MS = 100L;
    private static final long IDLE_SLEEP_NS = TimeUnit.MILLISECONDS.toNanos(IDLE_SLEEP_MS);

    private final SourceAction<T, SplitT, ?> sourceAction;
    private final TaskLocation enumeratorTaskLocation;

    private Address enumeratorTaskAddress;

    private SourceReader<T, SplitT> reader;

    private transient Serializer<SplitT> splitSerializer;

    private final int indexID;

    private final TaskLocation currentTaskLocation;

    private SeaTunnelSourceCollector<T> collector;

    private final MetricsContext metricsContext;
    private final EventListener eventListener;
    private SourceReader.Context context;

    private transient Counter sourceReadNs;
    private transient Counter sourceIdleNs;

    /** Timing metrics shared by the legacy reader path and the future managed runtime. */
    private transient SourceRuntimeMetrics sourceRuntimeMetrics;

    private final AtomicReference<SchemaChangePhase> schemaChangePhase = new AtomicReference<>();

    private final long flushIntervalMs;
    private final ManagedSourceRuntimeSelection runtimeSelection;

    private transient volatile ScheduledFuture<?> flushFuture;
    private transient ManagedSourceRuntimeConfig managedSourceRuntimeConfig;
    private transient ManagedSourceReaderRuntime<T, SplitT> managedReaderRuntime;

    public SourceFlowLifeCycle(
            SourceAction<T, SplitT, ?> sourceAction,
            int indexID,
            TaskLocation enumeratorTaskLocation,
            SeaTunnelTask runningTask,
            TaskLocation currentTaskLocation,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext,
            long flushIntervalMs) {
        this(
                sourceAction,
                indexID,
                enumeratorTaskLocation,
                runningTask,
                currentTaskLocation,
                completableFuture,
                metricsContext,
                flushIntervalMs,
                ManagedSourceRuntimeSelection.legacy());
    }

    public SourceFlowLifeCycle(
            SourceAction<T, SplitT, ?> sourceAction,
            int indexID,
            TaskLocation enumeratorTaskLocation,
            SeaTunnelTask runningTask,
            TaskLocation currentTaskLocation,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext,
            long flushIntervalMs,
            ManagedSourceRuntimeSelection runtimeSelection) {
        super(sourceAction, runningTask, completableFuture);
        this.sourceAction = sourceAction;
        this.indexID = indexID;
        this.enumeratorTaskLocation = enumeratorTaskLocation;
        this.currentTaskLocation = currentTaskLocation;
        this.metricsContext = metricsContext;
        this.flushIntervalMs = flushIntervalMs;
        this.runtimeSelection = runtimeSelection;
        this.eventListener =
                new JobEventListener(currentTaskLocation, runningTask.getExecutionContext());
    }

    /**
     * Initializes the source reader and supporting components.
     *
     * <p>This method creates the split serializer from the {@link SourceAction}, builds a {@link
     * SourceReaderContext} for the reader, creates the {@link SourceReader} instance, and resolves
     * the remote enumerator's network address.
     *
     * @throws Exception if reader creation or enumerator address resolution fails
     */
    @Override
    public void init() throws Exception {
        this.splitSerializer = sourceAction.getSource().getSplitSerializer();
        this.context =
                new SourceReaderContext(
                        indexID,
                        sourceAction.getSource().getBoundedness(),
                        this,
                        metricsContext,
                        eventListener);
        this.sourceReadNs = metricsContext.counter(SOURCE_READ_NANOS + "#" + sourceAction.getId());
        this.sourceIdleNs = metricsContext.counter(SOURCE_IDLE_NANOS + "#" + sourceAction.getId());
        if (runningTask.isObservabilityEnabled()) {
            this.sourceRuntimeMetrics =
                    new SourceRuntimeMetrics(metricsContext, sourceAction.getId());
        }
        this.managedSourceRuntimeConfig =
                runningTask
                        .getExecutionContext()
                        .getTaskExecutionService()
                        .getSeaTunnelConfig()
                        .getEngineConfig()
                        .getManagedSourceRuntimeConfig();
        this.reader = sourceAction.getSource().createReader(context);
        if (isManagedReaderRuntime() && !(reader instanceof ManagedSourceReader)) {
            throw new IllegalStateException(
                    "Source "
                            + sourceAction.getName()
                            + " selected the managed Reader lane but did not create a ManagedSourceReader");
        }
        this.enumeratorTaskAddress = getEnumeratorTaskAddress();
    }

    public void setCollector(SeaTunnelSourceCollector<T> collector) {
        this.collector = collector;
        if (isManagedReaderRuntime()) {
            @SuppressWarnings("unchecked")
            ManagedSourceReader<T, SplitT> managedReader = (ManagedSourceReader<T, SplitT>) reader;
            this.managedReaderRuntime =
                    new ManagedSourceReaderRuntime<>(
                            runningTask,
                            sourceAction,
                            currentTaskLocation,
                            enumeratorTaskLocation,
                            enumeratorTaskAddress,
                            managedReader,
                            splitSerializer,
                            collector,
                            managedSourceRuntimeConfig,
                            runtimeSelection);
        }
    }

    /**
     * Opens the source reader and registers this reader with the remote split enumerator.
     *
     * <p>Fires a {@link ReaderOpenEvent}, delegates to {@link SourceReader#open()}, and then calls
     * {@link #register()} to notify the enumerator that this reader is ready to receive splits.
     *
     * @throws Exception if the reader fails to open or registration fails
     */
    @Override
    public void open() throws Exception {
        context.getEventListener().onEvent(new ReaderOpenEvent());
        if (isManagedReaderRuntime()) {
            ((ManagedSourceReader<?, ?>) reader).activateManagedRuntime();
        }
        reader.open();
        if (isManagedReaderRuntime()) {
            if (managedReaderRuntime == null) {
                throw new IllegalStateException(
                        "Managed Source Reader runtime was not initialized with a collector");
            }
            managedReaderRuntime.start();
        } else {
            register();
        }
    }

    /**
     * Timer callback invoked by the {@code timerFlushWorker} thread pool.
     *
     * <p>Acquires the {@code checkpointLock} (the same monitor that {@link #triggerBarrier} uses)
     * so that flush signals and barriers are strictly serialized — a FlushSignal either completes
     * entirely before a Barrier or queues behind it, never crossing it.
     */
    private void onTimerTick() {
        if (prepareClose) {
            return;
        }
        try {
            if (isManagedReaderRuntime()) {
                managedReaderRuntime.admitFlushSignal(
                        currentTaskLocation.getJobId(), currentTaskLocation.getTaskID());
            } else {
                collector.sendFlushSignal(
                        currentTaskLocation.getJobId(), currentTaskLocation.getTaskID());
            }
        } catch (Exception e) {
            log.warn("Failed to broadcast FlushSignal from task {}", currentTaskLocation, e);
        }
    }

    private Address getEnumeratorTaskAddress() throws ExecutionException, InterruptedException {
        return (Address)
                runningTask
                        .getExecutionContext()
                        .sendToMaster(new GetTaskGroupAddressOperation(enumeratorTaskLocation))
                        .get();
    }

    @Override
    public void close() throws IOException {
        try {
            if (managedReaderRuntime != null) {
                managedReaderRuntime.close();
            }
            context.getEventListener().onEvent(new ReaderCloseEvent());
            reader.close();
            super.close();
        } finally {
            closeFlushTimer();
        }
    }

    @Override
    public void hook() throws IOException {
        startFlushTimer();
    }

    /**
     * Core read loop that polls the source reader for the next batch of records.
     *
     * <p>This method is called repeatedly by the task execution loop. It performs the following:
     *
     * <ol>
     *   <li>If {@code prepareClose} is set, the reader is shutting down and this method sleeps to
     *       yield the thread.
     *   <li>If a schema change is in progress, reading is paused until the schema-change checkpoint
     *       completes.
     *   <li>Otherwise, calls {@link SourceReader#pollNext} to fetch records. If no records were
     *       produced, sleeps briefly to avoid busy-waiting.
     *   <li>After polling, checks for schema-change signals from the collector. If a before or
     *       after schema-change signal is captured, it initiates the corresponding schema-change
     *       checkpoint phase and pauses further collection until the checkpoint completes.
     * </ol>
     *
     * <p><b>Checkpoint lock interaction:</b> The reader holds the checkpoint lock during {@code
     * pollNext}. A brief {@code Thread.sleep(0L)} after a non-empty poll gives the checkpoint
     * thread a chance to acquire the lock via {@link #triggerBarrier(Barrier)}, preventing
     * checkpoint starvation under high CPU load.
     *
     * @throws Exception if polling or schema-change triggering fails
     */
    public void collect() throws Exception {
        if (isManagedReaderRuntime()) {
            if (!managedReaderRuntime.runOneTurn()) {
                Thread.sleep(managedSourceRuntimeConfig.getIdleWaitMillis());
            }
            return;
        }
        boolean metricsEnabled = sourceRuntimeMetrics != null;
        if (!prepareClose) {
            if (schemaChanging()) {
                log.debug("schema is changing, stop reader collect records");
                if (metricsEnabled) {
                    sourceIdleNs.inc(SCHEMA_CHANGE_SLEEP_NS);
                }
                Thread.sleep(SCHEMA_CHANGE_SLEEP_MS);
                return;
            }

            collector.resetEmptyThisPollNext();
            long startNs = metricsEnabled ? System.nanoTime() : 0L;
            long pollCostNs = 0L;
            try {
                reader.pollNext(collector);
            } finally {
                if (metricsEnabled) {
                    pollCostNs = System.nanoTime() - startNs;
                    sourceRuntimeMetrics.recordPoll(pollCostNs);
                }
            }
            if (collector.isEmptyThisPollNext()) {
                if (metricsEnabled) {
                    sourceIdleNs.inc(pollCostNs);
                    sourceIdleNs.inc(IDLE_SLEEP_NS);
                }
                Thread.sleep(IDLE_SLEEP_MS);
            } else {
                if (metricsEnabled) {
                    sourceReadNs.inc(pollCostNs);
                }
                collector.resetEmptyThisPollNext();
                /*
                 * The current thread obtain a checkpoint lock in the method {@link
                 * SourceReader#pollNext(Collector)}. When trigger the checkpoint or savepoint,
                 * other threads try to obtain the lock in the method {@link
                 * SourceFlowLifeCycle#triggerBarrier(Barrier)}. When high CPU load, checkpoint
                 * process may be blocked as long time. So we need sleep to free the CPU.
                 */
                Thread.sleep(0L);
            }

            if (collector.captureSchemaChangeBeforeCheckpointSignal()) {
                if (schemaChangePhase.get() != null) {
                    throw new IllegalStateException(
                            "previous schema changes in progress, schemaChangePhase: "
                                    + schemaChangePhase.get());
                }
                schemaChangePhase.set(SchemaChangePhase.createBeforePhase());
                runningTask.triggerSchemaChangeBeforeCheckpoint().get();
                log.info("triggered schema-change-before checkpoint, stopping collect data");
            } else if (collector.captureSchemaChangeAfterCheckpointSignal()) {
                if (schemaChangePhase.get() != null) {
                    throw new IllegalStateException(
                            "previous schema changes in progress, schemaChangePhase: "
                                    + schemaChangePhase.get());
                }
                schemaChangePhase.set(SchemaChangePhase.createAfterPhase());
                runningTask.triggerSchemaChangeAfterCheckpoint().get();
                log.info("triggered schema-change-after checkpoint, stopping collect data");
            }
        } else {
            if (metricsEnabled) {
                sourceIdleNs.inc(IDLE_SLEEP_NS);
            }
            Thread.sleep(IDLE_SLEEP_MS);
        }
    }

    /**
     * Signals that this reader has no more data to produce.
     *
     * <p>Sets the {@code prepareClose} flag to {@code true} and sends a {@link
     * SourceNoMoreElementOperation} to the remote enumerator, deregistering this reader from
     * further split assignment.
     *
     * @throws RuntimeException if the deregistration message fails to send
     */
    public void signalNoMoreElement() {
        if (isManagedReaderRuntime()) {
            this.prepareClose = true;
            managedReaderRuntime.signalNoMoreElement();
            return;
        }
        // ready close this reader
        try {
            this.prepareClose = true;
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SourceNoMoreElementOperation(
                                    currentTaskLocation, enumeratorTaskLocation),
                            enumeratorTaskAddress)
                    .get();
        } catch (Exception e) {
            log.warn("source close failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Registers this reader with the remote split enumerator.
     *
     * <p>Sends a {@link SourceRegisterOperation} to the enumerator at the previously resolved
     * address, informing it that this reader subtask is ready to receive splits.
     *
     * @throws RuntimeException if registration fails due to communication errors
     */
    private void register() {
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SourceRegisterOperation(
                                    currentTaskLocation, enumeratorTaskLocation),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source register failed.", e);
            throw new RuntimeException(e);
        }
    }

    private void startFlushTimer() {
        if (flushIntervalMs <= 0) {
            return;
        }
        flushFuture =
                runningTask
                        .getExecutionContext()
                        .getTaskExecutionService()
                        .registerTimerFlushTask(
                                currentTaskLocation, this::onTimerTick, flushIntervalMs);
        log.info(
                "Registered flush timer for source task {}, intervalMs={}",
                currentTaskLocation,
                flushIntervalMs);
    }

    private void closeFlushTimer() {
        if (flushFuture == null) {
            return;
        }
        try {
            runningTask
                    .getExecutionContext()
                    .getTaskExecutionService()
                    .closeTimerFlushTask(currentTaskLocation);
        } catch (Exception e) {
            log.warn("Failed to close flush timer for task {}", currentTaskLocation, e);
        }
        flushFuture = null;
    }

    /**
     * Sends a split request to the remote split enumerator.
     *
     * <p>Sends a {@link RequestSplitOperation} to the enumerator, requesting new splits to be
     * assigned to this reader. The enumerator will respond asynchronously by calling {@link
     * #receivedSplits(List)}.
     *
     * @throws RuntimeException if the split request fails due to communication errors
     */
    public void requestSplit() {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.requestSplit();
            return;
        }
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new RequestSplitOperation(currentTaskLocation, enumeratorTaskLocation),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source request split failed.", e);
            throw new RuntimeException(e);
        }
    }

    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.sendSourceEvent(sourceEvent);
            return;
        }
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new SourceReaderEventOperation(
                                    enumeratorTaskLocation, currentTaskLocation, sourceEvent),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source request split failed.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Handles splits received from the remote split enumerator.
     *
     * <p>If the split list is empty, it indicates that the enumerator has no more splits to assign,
     * and {@link SourceReader#handleNoMoreSplits()} is called. Otherwise, the splits are forwarded
     * to the reader via {@link SourceReader#addSplits(List)}.
     *
     * @param splits the list of splits assigned by the enumerator; an empty list signals no more
     *     splits
     */
    public void receivedSplits(List<SplitT> splits) {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.admitLegacySplits(splits);
            return;
        }
        boolean metricsEnabled = sourceRuntimeMetrics != null;
        long startNanos = metricsEnabled ? System.nanoTime() : 0L;
        try {
            if (splits.isEmpty()) {
                reader.handleNoMoreSplits();
            } else {
                reader.addSplits(splits);
            }
        } finally {
            if (metricsEnabled) {
                sourceRuntimeMetrics.recordReaderCallback(System.nanoTime() - startNanos);
            }
        }
    }

    public void receivedSerializedSplits(List<byte[]> splits) {
        if (!isManagedReaderRuntime()) {
            throw new IllegalStateException(
                    "Serialized split admission is only valid in the managed Reader lane");
        }
        managedReaderRuntime.admitSerializedLegacySplits(splits);
    }

    /**
     * Injects a checkpoint barrier into the record stream.
     *
     * <p>This method acquires the {@code checkpointLock} on the collector to ensure mutual
     * exclusion with the reader's {@code pollNext} calls. While holding the lock, it:
     *
     * <ol>
     *   <li>Propagates the {@code prepareClose} flag if the barrier targets this task
     *   <li>Snapshots the reader state (if the barrier requires a snapshot) and registers it with
     *       the running task
     *   <li>Acknowledges the barrier and sends it downstream as a {@link Record}
     * </ol>
     *
     * <p>After releasing the lock, if the barrier carries a schema-change checkpoint type, the
     * method associates the barrier's checkpoint ID with the current {@link SchemaChangePhase}.
     * This locks the collect loop until the schema-change checkpoint completes or is aborted.
     *
     * @param barrier the checkpoint or savepoint barrier to inject
     * @throws Exception if state snapshotting or barrier acknowledgment fails
     */
    public void triggerBarrier(Barrier barrier) throws Exception {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.admitBarrier(barrier);
            return;
        }
        log.debug("source trigger barrier [{}]", barrier);

        long startTime = System.currentTimeMillis();
        boolean metricsEnabled = sourceRuntimeMetrics != null;
        long checkpointStartNanos = metricsEnabled ? System.nanoTime() : 0L;

        // Block the reader from adding barrier to the collector.
        synchronized (collector.getCheckpointLock()) {
            if (metricsEnabled) {
                sourceRuntimeMetrics.recordCheckpointLockWait(
                        System.nanoTime() - checkpointStartNanos);
            }
            if (barrier.prepareClose(this.currentTaskLocation)) {
                this.prepareClose = true;
            }
            if (barrier.snapshot()) {
                long snapshotStartNanos = metricsEnabled ? System.nanoTime() : 0L;
                try {
                    List<byte[]> states =
                            serializeStates(splitSerializer, reader.snapshotState(barrier.getId()));
                    runningTask.addState(barrier, ActionStateKey.of(sourceAction), states);
                } finally {
                    if (metricsEnabled) {
                        sourceRuntimeMetrics.recordCheckpointSnapshot(
                                System.nanoTime() - snapshotStartNanos);
                    }
                }
            }
            long barrierForwardStartNanos = metricsEnabled ? System.nanoTime() : 0L;
            try {
                // ack after #addState
                runningTask.ack(barrier);
                log.debug("source ack barrier finished, taskId: [{}]", runningTask.getTaskID());
                collector.sendRecordToNext(new Record<>(barrier));
                log.debug("send record to next finished, taskId: [{}]", runningTask.getTaskID());
            } finally {
                if (metricsEnabled) {
                    sourceRuntimeMetrics.recordBarrierForward(
                            System.nanoTime() - barrierForwardStartNanos);
                }
            }
        }

        log.debug(
                "trigger barrier [{}] finished, cost: {}ms. taskLocation: [{}]",
                barrier.getId(),
                System.currentTimeMillis() - startTime,
                currentTaskLocation);

        CheckpointType checkpointType = ((CheckpointBarrier) barrier).getCheckpointType();
        if (checkpointType.isSchemaChangeCheckpoint()) {
            if (schemaChanging()) {
                if (checkpointType.isSchemaChangeBeforeCheckpoint()
                        && schemaChangePhase.get().isBeforePhase()) {
                    schemaChangePhase.get().setCheckpointId(barrier.getId());
                } else if (checkpointType.isSchemaChangeAfterCheckpoint()
                        && schemaChangePhase.get().isAfterPhase()) {
                    schemaChangePhase.get().setCheckpointId(barrier.getId());
                } else {
                    throw new IllegalStateException(
                            String.format(
                                    "schema-change checkpoint[%s,%s] and phase[%s] is not matched",
                                    barrier.getId(),
                                    checkpointType,
                                    schemaChangePhase.get().getPhase()));
                }
                log.info(
                        "lock checkpoint[{}] waiting for complete..., phase: [{}]",
                        barrier.getId(),
                        schemaChangePhase.get().getPhase());
            } else {
                log.debug(
                        "Ignore schema-change checkpoint[{}] on idle task, phase: [{}]",
                        barrier.getId(),
                        checkpointType);
            }
        }
    }

    private boolean schemaChanging() {
        return schemaChangePhase.get() != null;
    }

    /**
     * Notifies the source reader that a checkpoint has been successfully completed.
     *
     * <p>Delegates to {@link SourceReader#notifyCheckpointComplete(long)}, allowing the connector
     * to perform post-commit cleanup such as acknowledging consumed offsets or removing temporary
     * files.
     *
     * @param checkpointId the ID of the completed checkpoint
     * @throws Exception if the reader's post-checkpoint hook fails
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.admitCheckpointComplete(checkpointId);
            return;
        }
        boolean metricsEnabled = sourceRuntimeMetrics != null;
        long startNanos = metricsEnabled ? System.nanoTime() : 0L;
        try {
            reader.notifyCheckpointComplete(checkpointId);
        } finally {
            if (metricsEnabled) {
                sourceRuntimeMetrics.recordReaderCallback(System.nanoTime() - startNanos);
            }
        }
    }

    /**
     * Notifies the source reader that a checkpoint has been aborted.
     *
     * <p>Delegates to {@link SourceReader#notifyCheckpointAborted(long)} and then checks whether
     * the aborted checkpoint matches an in-progress schema-change phase. If so, an {@link
     * IllegalStateException} is thrown because a schema-change checkpoint cannot be safely retried
     * once aborted.
     *
     * @param checkpointId the ID of the aborted checkpoint
     * @throws IllegalStateException if the aborted checkpoint is a schema-change checkpoint
     * @throws Exception if the reader's abort notification hook fails
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.admitCheckpointAborted(checkpointId);
            return;
        }
        boolean metricsEnabled = sourceRuntimeMetrics != null;
        long startNanos = metricsEnabled ? System.nanoTime() : 0L;
        try {
            reader.notifyCheckpointAborted(checkpointId);
        } finally {
            if (metricsEnabled) {
                sourceRuntimeMetrics.recordReaderCallback(System.nanoTime() - startNanos);
            }
        }
        if (schemaChangePhase.get() != null
                && schemaChangePhase.get().getCheckpointId() == checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "schema-change checkpoint[%s] is aborted, phase: [%s]",
                            checkpointId, schemaChangePhase.get().getPhase()));
        }
    }

    @Override
    public void notifyCheckpointEnd(long checkpointId) throws Exception {
        if (isManagedReaderRuntime()) {
            managedReaderRuntime.admitCheckpointEnd(checkpointId);
            return;
        }
        if (schemaChangePhase.get() != null
                && schemaChangePhase.get().getCheckpointId() == checkpointId) {
            log.info(
                    "notify schema-change checkpoint[{}] end, phase: [{}]",
                    checkpointId,
                    schemaChangePhase.get().getPhase());
            schemaChangePhase.set(null);
        }
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        if (actionStateList.isEmpty()) {
            return;
        }
        List<byte[]> splits =
                actionStateList.stream()
                        .map(ActionSubtaskState::getState)
                        .flatMap(Collection::stream)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        if (isManagedReaderRuntime()) {
            if (splits.stream()
                    .anyMatch(
                            state ->
                                    !ManagedReaderCheckpointStateSerializer.isManagedState(
                                            state))) {
                throw new IllegalStateException(
                        "Cannot restore a managed Source Reader from legacy checkpoint state");
            }
            List<ManagedReaderCheckpointState> restoredStates = new java.util.ArrayList<>();
            for (byte[] state : splits) {
                ManagedReaderCheckpointState restored =
                        ManagedReaderCheckpointStateSerializer.deserialize(state);
                restoredStates.add(restored);
            }
            managedReaderRuntime.restoreMetadata(restoredStates);
            // The managed Reader replays connector state and ownership proofs through its fenced
            // Reader-to-coordinator channel before acknowledging the new coordinator epoch.
            return;
        } else if (splits.stream()
                .anyMatch(ManagedReaderCheckpointStateSerializer::isManagedState)) {
            throw new IllegalStateException(
                    "Cannot silently restore managed Source Reader state in the legacy lane");
        }
        try {
            runningTask
                    .getExecutionContext()
                    .sendToMember(
                            new RestoredSplitOperation(enumeratorTaskLocation, splits, indexID),
                            enumeratorTaskAddress)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("source request split failed.", e);
            throw new RuntimeException(e);
        }
    }

    public SourceCommandAdmissionAck admitManagedSourceCommand(SourceCommandEnvelope command) {
        if (!isManagedReaderRuntime() || managedReaderRuntime == null) {
            return SourceCommandAdmissionAck.of(
                    SourceCommandAdmissionStatus.STALE_TARGET,
                    command,
                    -1L,
                    0L,
                    "Source Reader is in the legacy lane");
        }
        return managedReaderRuntime.admitCommand(command);
    }

    /** Propagates an asynchronous checkpoint transport failure to the managed Reader owner. */
    public void reportManagedRuntimeFailure(Throwable failure) {
        if (!isManagedReaderRuntime() || managedReaderRuntime == null) {
            throw new IllegalStateException(
                    "Managed Source runtime failure reported to the legacy lane", failure);
        }
        managedReaderRuntime.reportAsynchronousFailure(failure);
    }

    public boolean isManagedReaderRuntime() {
        return runtimeSelection != null && runtimeSelection.getMode().hasManagedReader();
    }

    @Getter
    @ToString
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SchemaChangePhase implements Serializable {
        private static final String PHASE_CHANGE_BEFORE = "SCHEMA-CHANGE-BEFORE";
        private static final String PHASE_CHANGE_AFTER = "SCHEMA-CHANGE-AFTER";

        private final String phase;
        private volatile long checkpointId = -1;

        public static SchemaChangePhase createBeforePhase() {
            return new SchemaChangePhase(PHASE_CHANGE_BEFORE);
        }

        public static SchemaChangePhase createAfterPhase() {
            return new SchemaChangePhase(PHASE_CHANGE_AFTER);
        }

        public boolean isBeforePhase() {
            return PHASE_CHANGE_BEFORE.equals(phase);
        }

        public boolean isAfterPhase() {
            return PHASE_CHANGE_AFTER.equals(phase);
        }

        public void setCheckpointId(long checkpointId) {
            if (this.checkpointId != -1) {
                throw new IllegalStateException("checkpointId is already set");
            }
            this.checkpointId = checkpointId;
        }
    }
}

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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.common.error.RowErrorHandlingFatalException;
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.common.constants.JobMode;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A composite {@link SinkWriter} that distributes rows to multiple per-table sub-writers via async
 * blocking queues.
 *
 * <p>Incoming rows are routed to one of {@code queueSize} {@link BlockingQueue}s. Each queue is
 * consumed by a dedicated {@link MultiTableWriterRunnable} thread that dispatches rows to the
 * appropriate sub-writer. Routing is based on the row's primary key hash for ordered delivery, or
 * random selection when no primary key exists.
 *
 * <p>Thread pool sizing is {@code queueSize * 2}: half for queue consumer threads, half for
 * parallel {@link #prepareCommit(long)} tasks. Synchronization between the consumer threads and
 * checkpoint/commit operations is coordinated through locks on each {@link
 * MultiTableWriterRunnable} instance.
 */
@Slf4j
public class MultiTableSinkWriter
        implements SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState>,
                SupportSchemaEvolutionSinkWriter {

    private static final long EXECUTOR_CLOSE_TIMEOUT_SECONDS = 60L;

    private final Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters;
    private final Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext;
    private final ConcurrentMap<String, Optional<Integer>> sinkPrimaryKeys =
            new ConcurrentHashMap<>();
    private final List<ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>>
            sinkWritersWithIndex;
    private final List<MultiTableWriterRunnable> runnable = new ArrayList<>();
    private final Random random = new Random();
    private final List<BlockingQueue<MultiTableWriterRunnable.QueueElement>> blockingQueues =
            new ArrayList<>();
    private final ExecutorService executorService;
    private final MultiTableFailurePolicy failurePolicy;
    private final JobMode jobMode;
    private final int tableRetryTimes;
    private final int tableRetryIntervalSeconds;
    private final Map<String, List<SinkIdentifier>> sinkIdentifiersByTable =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MultiTableFailedTable> failedTables =
            new ConcurrentHashMap<>();
    private MultiTableResourceManager resourceManager;
    private volatile MultiTableRowErrorHandler rowErrorHandler;
    private volatile boolean submitted = false;
    private volatile Throwable fatalThrowable;
    private volatile String fatalTableId;

    /**
     * Creates a MultiTableSinkWriter that distributes writes across multiple queues.
     *
     * <p>Initializes a fixed thread pool of size {@code queueSize * 2}: half the threads run {@link
     * MultiTableWriterRunnable} consumers that drain the blocking queues, and the other half are
     * reserved for parallel {@link #prepareCommit(long)} tasks. Each sub-writer is assigned to a
     * queue using the formula {@code sinkIdentifier.getIndex() % queueSize}, ensuring deterministic
     * distribution across queues.
     *
     * @param sinkWriters map of sink identifiers to their corresponding writers
     * @param queueSize number of blocking queues (and consumer threads) to create
     * @param sinkWritersContext map of sink identifiers to their writer contexts
     */
    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext) {
        this(
                sinkWriters,
                queueSize,
                sinkWritersContext,
                MultiTableFailurePolicy.FAIL_FAST,
                JobMode.BATCH,
                Collections.emptyList(),
                0,
                0);
    }

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext,
            MultiTableFailurePolicy failurePolicy,
            JobMode jobMode) {
        this(
                sinkWriters,
                queueSize,
                sinkWritersContext,
                failurePolicy,
                jobMode,
                Collections.emptyList(),
                0,
                0);
    }

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext,
            MultiTableFailurePolicy failurePolicy,
            JobMode jobMode,
            int tableRetryTimes,
            int tableRetryIntervalSeconds) {
        this(
                sinkWriters,
                queueSize,
                sinkWritersContext,
                failurePolicy,
                jobMode,
                Collections.emptyList(),
                tableRetryTimes,
                tableRetryIntervalSeconds);
    }

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext,
            MultiTableFailurePolicy failurePolicy,
            JobMode jobMode,
            Collection<MultiTableFailedTable> initialFailedTables) {
        this(
                sinkWriters,
                queueSize,
                sinkWritersContext,
                failurePolicy,
                jobMode,
                initialFailedTables,
                0,
                0);
    }

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext,
            MultiTableFailurePolicy failurePolicy,
            JobMode jobMode,
            Collection<MultiTableFailedTable> initialFailedTables,
            int tableRetryTimes,
            int tableRetryIntervalSeconds) {
        this.sinkWriters = sinkWriters;
        this.sinkWritersContext = sinkWritersContext;
        this.failurePolicy = failurePolicy;
        this.jobMode = jobMode;
        this.tableRetryTimes = Math.max(0, tableRetryTimes);
        this.tableRetryIntervalSeconds = Math.max(0, tableRetryIntervalSeconds);
        AtomicInteger cnt = new AtomicInteger(0);
        executorService =
                MDCTracer.tracing(
                        Executors.newFixedThreadPool(
                                // we use it in `MultiTableWriterRunnable` and `prepare commit
                                // task`, so it
                                // should be double.
                                queueSize * 2,
                                runnable -> {
                                    Thread thread = new Thread(runnable);
                                    thread.setDaemon(true);
                                    thread.setName(
                                            "st-multi-table-sink-writer"
                                                    + "-"
                                                    + cnt.incrementAndGet());
                                    return thread;
                                }));
        sinkWritersWithIndex = new ArrayList<>();
        for (int i = 0; i < queueSize; i++) {
            BlockingQueue<MultiTableWriterRunnable.QueueElement> queue =
                    new LinkedBlockingQueue<>(1024);
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap = new HashMap<>();
            ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierMap =
                    new ConcurrentHashMap<>();
            int queueIndex = i;
            sinkWriters.entrySet().stream()
                    .filter(entry -> entry.getKey().getIndex() % queueSize == queueIndex)
                    .forEach(
                            entry -> {
                                tableIdWriterMap.put(
                                        entry.getKey().getTableIdentifier(), entry.getValue());
                                sinkIdentifierMap.put(entry.getKey(), entry.getValue());
                                sinkIdentifiersByTable
                                        .computeIfAbsent(
                                                entry.getKey().getTableIdentifier(),
                                                key ->
                                                        Collections.synchronizedList(
                                                                new ArrayList<>()))
                                        .add(entry.getKey());
                            });

            sinkWritersWithIndex.add(sinkIdentifierMap);
            blockingQueues.add(queue);
            MultiTableWriterRunnable r =
                    new MultiTableWriterRunnable(
                            tableIdWriterMap,
                            queue,
                            failurePolicy.continueOtherTables(),
                            this::handleRuntimeTableFailure,
                            this.tableRetryTimes,
                            this.tableRetryIntervalSeconds);
            runnable.add(r);
        }
        log.info("init multi table sink writer, queue size: {}", queueSize);
        initResourceManager(queueSize);
        registerInitialFailedTables(initialFailedTables);
    }

    public void setRowErrorHandler(MultiTableRowErrorHandler rowErrorHandler) {
        this.rowErrorHandler = rowErrorHandler;
        for (MultiTableWriterRunnable writerRunnable : runnable) {
            writerRunnable.setRowErrorHandler(rowErrorHandler);
        }
    }

    public void setWriteSuccessHandler(Consumer<SeaTunnelRow> writeSuccessHandler) {
        for (MultiTableWriterRunnable writerRunnable : runnable) {
            writerRunnable.setWriteSuccessHandler(writeSuccessHandler);
        }
    }

    /**
     * Initializes the shared {@link MultiTableResourceManager} from the first available writer and
     * broadcasts it to all sub-writers.
     *
     * <p>This is a one-shot initialization: the resource manager is created by the first writer via
     * {@link SupportMultiTableSinkWriter#initMultiTableResourceManager}, then distributed to every
     * writer through {@link SupportMultiTableSinkWriter#setMultiTableResourceManager}. Also
     * populates the {@link #sinkPrimaryKeys} map used for hash-based row routing in {@link
     * #write(SeaTunnelRow)}.
     *
     * @param queueSize the number of queues, passed to the resource manager for sizing
     */
    private void initResourceManager(int queueSize) {
        for (SinkIdentifier tableIdentifier : sinkWriters.keySet()) {
            SinkWriter<SeaTunnelRow, ?, ?> sink = sinkWriters.get(tableIdentifier);
            resourceManager =
                    ((SupportMultiTableSinkWriter<?>) sink)
                            .initMultiTableResourceManager(sinkWriters.size(), queueSize);
            break;
        }

        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writerMap =
                    sinkWritersWithIndex.get(i);
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> entry :
                    writerMap.entrySet()) {
                SupportMultiTableSinkWriter<?> sink =
                        ((SupportMultiTableSinkWriter<?>) entry.getValue());
                sink.setMultiTableResourceManager(resourceManager, i);
                sinkPrimaryKeys.put(entry.getKey().getTableIdentifier(), sink.primaryKey());
            }
        }
    }

    /**
     * Checks all async writer threads for errors and propagates any exception to the caller.
     *
     * <p>Each {@link MultiTableWriterRunnable} captures exceptions thrown during write into a
     * volatile field. This method iterates over all runnables and re-throws the first error found
     * through the checked {@link IOException} contract already declared by the schema-change, write
     * and snapshot APIs.
     */
    private void subSinkErrorCheck() throws IOException {
        IOException failure = currentSubSinkFailure();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Converts the first observed queue-worker failure into {@link IOException} so coordinator
     * entry points keep one consistent failure channel.
     */
    private IOException currentSubSinkFailure() {
        if (fatalThrowable != null) {
            return buildIOException(buildFailureSummary(), fatalThrowable);
        }
        for (MultiTableWriterRunnable writerRunnable : runnable) {
            Throwable throwable = writerRunnable.getThrowable();
            if (throwable != null) {
                if (throwable instanceof IOException) {
                    return (IOException) throwable;
                }
                return new IOException(
                        String.format(
                                "table %s sink throw error", writerRunnable.getCurrentTableId()),
                        throwable);
            }
        }
        if (failurePolicy.continueOtherTables()
                && sinkPrimaryKeys.isEmpty()
                && !failedTables.isEmpty()) {
            return buildIOException(buildIsolatedFailureSummary(), getFirstFailureCause());
        }
        return null;
    }

    /**
     * Wraps non-IO failures so callers that already rely on {@link IOException} can keep one
     * predictable error type.
     */
    private IOException buildIOException(String message, Throwable failure) {
        if (failure instanceof IOException && message.equals(failure.getMessage())) {
            return (IOException) failure;
        }
        return new IOException(message, failure);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        subSinkErrorCheck();
        if (!hasSourceMatchedWriter(event)) {
            return;
        }
        ensureQueueWorkersSubmitted();
        subSinkErrorCheck();
        enqueueSchemaChangeBarrier(event);
    }

    /**
     * Keeps the schema-change path on the legacy source-table contract so unrelated table events
     * can still return immediately without waking queue workers.
     */
    private boolean hasSourceMatchedWriter(SchemaChangeEvent event) {
        String tableId = event.tablePath().getFullName();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (sinkWriterEntry.getKey().getTableIdentifier().equals(tableId)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Collects every sub-writer that must observe this schema change event. We first route by the
     * source table identifier carried by the event, then fan the same event out to sibling
     * sub-writers that report the same physical sink table identifier.
     */
    private List<SchemaChangeDispatchTarget> collectSchemaChangeDispatchTargets(
            SchemaChangeEvent event) {
        String tableId = event.tablePath().getFullName();
        if (isTableFailed(tableId)) {
            log.warn("Skip schema change for failed table {}", tableId);
            return Collections.emptyList();
        }

        Set<String> primarySharedSinkIds = new HashSet<>();
        Set<SinkIdentifier> primaryDispatchedKeys = new HashSet<>();
        List<SchemaChangeDispatchTarget> dispatchTargets = new ArrayList<>();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (sinkWriterEntry.getKey().getTableIdentifier().equals(tableId)) {
                    dispatchTargets.add(
                            new SchemaChangeDispatchTarget(
                                    sinkWriterEntry.getKey(),
                                    sinkWriterEntry.getValue(),
                                    "source-match"));
                    primaryDispatchedKeys.add(sinkWriterEntry.getKey());
                    extractPhysicalSinkIdentifier(sinkWriterEntry.getValue())
                            .ifPresent(primarySharedSinkIds::add);
                }
            }
        }
        if (primarySharedSinkIds.isEmpty()) {
            return dispatchTargets;
        }

        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (primaryDispatchedKeys.contains(sinkWriterEntry.getKey())
                        || isTableFailed(sinkWriterEntry.getKey().getTableIdentifier())) {
                    continue;
                }
                Optional<String> siblingPhysicalSinkId =
                        extractPhysicalSinkIdentifier(sinkWriterEntry.getValue());
                if (siblingPhysicalSinkId.isPresent()
                        && primarySharedSinkIds.contains(siblingPhysicalSinkId.get())) {
                    dispatchTargets.add(
                            new SchemaChangeDispatchTarget(
                                    sinkWriterEntry.getKey(),
                                    sinkWriterEntry.getValue(),
                                    "shared-physical-sink " + siblingPhysicalSinkId.get()));
                }
            }
        }
        return dispatchTargets;
    }

    /**
     * Routes schema changes through the same queue workers as data rows. Every queue must first
     * drain older rows, then all workers stop at the shared barrier, and only then does one worker
     * mutate the shared sink schema.
     */
    private void enqueueSchemaChangeBarrier(SchemaChangeEvent event) throws IOException {
        List<SchemaChangeFailure> schemaChangeFailures =
                Collections.synchronizedList(new ArrayList<>());
        SchemaChangeBarrier schemaChangeBarrier =
                new SchemaChangeBarrier(
                        event,
                        runnable.size(),
                        schemaChangeEvent ->
                                dispatchSchemaChangeToTargets(
                                        schemaChangeEvent, schemaChangeFailures));
        try {
            for (BlockingQueue<MultiTableWriterRunnable.QueueElement> blockingQueue :
                    blockingQueues) {
                offerQueueElement(
                        blockingQueue,
                        MultiTableWriterRunnable.schemaChangeRequest(schemaChangeBarrier));
            }
            // A worker can fail after applySchemaChange's last error check but before its barrier
            // request is enqueued. In that window the worker has already drained its queue and
            // cannot fail the newly enqueued request, so recheck only after every request is
            // visible to close the lost-notification race.
            subSinkErrorCheck();
        } catch (Throwable error) {
            schemaChangeBarrier.fail(error);
            throwAsIOException(error);
        }
        schemaChangeBarrier.awaitCompletion();
        for (SchemaChangeFailure schemaChangeFailure : schemaChangeFailures) {
            handleTableFailure(
                    schemaChangeFailure.getTableId(),
                    MultiTableFailurePhase.CHECKPOINT,
                    schemaChangeFailure.getError());
        }
        waitRuntimeTableFailuresHandled();
        subSinkErrorCheck();
    }

    /**
     * Runs the final schema-change fan-out only after every queue worker has already drained older
     * rows and reached the shared barrier.
     */
    private void dispatchSchemaChangeToTargets(
            SchemaChangeEvent event, List<SchemaChangeFailure> schemaChangeFailures)
            throws IOException {
        List<SchemaChangeDispatchTarget> dispatchTargets =
                collectSchemaChangeDispatchTargets(event);
        for (SchemaChangeDispatchTarget dispatchTarget : dispatchTargets) {
            try {
                applySchemaChangeToTarget(event, dispatchTarget);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throwAsIOException(error);
            } catch (Throwable error) {
                if (failurePolicy.continueOtherTables()) {
                    schemaChangeFailures.add(
                            new SchemaChangeFailure(
                                    dispatchTarget.getSinkIdentifier().getTableIdentifier(),
                                    error));
                    continue;
                }
                throwAsIOException(error);
            }
        }
    }

    /**
     * Applies the schema change to one sub-writer while every queue worker is already waiting on
     * the shared schema-change barrier.
     */
    private void applySchemaChangeToTarget(
            SchemaChangeEvent event, SchemaChangeDispatchTarget dispatchTarget) throws Throwable {
        log.info(
                "Start apply schema change for table {} sub-writer {} ({})",
                dispatchTarget.getSinkIdentifier().getTableIdentifier(),
                dispatchTarget.getSinkIdentifier().getIndex(),
                dispatchTarget.getReason());
        executeWithTableRetry(
                dispatchTarget.getSinkIdentifier().getTableIdentifier(),
                MultiTableFailurePhase.CHECKPOINT,
                () -> {
                    if (dispatchTarget.getWriter() instanceof SupportSchemaEvolutionSinkWriter) {
                        ((SupportSchemaEvolutionSinkWriter) dispatchTarget.getWriter())
                                .applySchemaChange(event);
                    } else {
                        // TODO remove deprecated method
                        dispatchTarget.getWriter().applySchemaChange(event);
                    }
                    return null;
                });
        log.info(
                "Finish apply schema change for table {} sub-writer {} ({})",
                dispatchTarget.getSinkIdentifier().getTableIdentifier(),
                dispatchTarget.getSinkIdentifier().getIndex(),
                dispatchTarget.getReason());
    }

    private Optional<String> extractPhysicalSinkIdentifier(SinkWriter<SeaTunnelRow, ?, ?> writer) {
        if (writer instanceof SupportSchemaEvolutionSinkWriter) {
            Optional<String> physicalSinkIdentifier =
                    ((SupportSchemaEvolutionSinkWriter) writer).getPhysicalSinkTableIdentifier();
            return physicalSinkIdentifier == null ? Optional.empty() : physicalSinkIdentifier;
        }
        return Optional.empty();
    }

    /**
     * Routes a row to the appropriate blocking queue for async writing.
     *
     * <p>On the first call, lazily starts all {@link MultiTableWriterRunnable} consumer threads via
     * the executor service (controlled by the {@code submitted} flag).
     *
     * <p>Row routing strategy:
     *
     * <ul>
     *   <li>If the table's primary key information is present and the primary key field value is
     *       non-null, the row is routed by {@code (primaryKeyValue.hashCode() & Integer.MAX_VALUE)
     *       % queueSize}, guaranteeing that rows with the same primary key always go to the same
     *       queue for ordered delivery.
     *   <li>If the table's primary key information is present but the actual field value is {@code
     *       null}, the row is routed to queue 0.
     *   <li>If the table has no primary key or this is a single-table sink, the row is sent to a
     *       randomly selected queue for load balancing.
     *   <li>If the table's primary key metadata is missing (not initialized) in multi-table mode, a
     *       {@link RuntimeException} is thrown.
     * </ul>
     *
     * <p>If the target queue is full, blocks with 500ms timeout retries, checking for sub-sink
     * errors between attempts.
     *
     * @param element the row to write
     * @throws IOException if interrupted while waiting for queue capacity
     */
    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (element != null && element.getOptions() != null) {
            if (element.getOptions().containsKey("flush_event")
                    || element.getOptions().containsKey("schema_change_event")) {
                log.debug("Skipping schema change event row: {}", element.getOptions().keySet());
                return;
            }
        }

        ensureQueueWorkersSubmitted();
        subSinkErrorCheck();
        String tableId = element.getTableId();
        if (failurePolicy.continueOtherTables() && isTableFailed(tableId)) {
            log.debug("Skip row for quarantined table {}", tableId);
            return;
        }
        Optional<Integer> primaryKey = tableId == null ? null : sinkPrimaryKeys.get(tableId);
        if ((primaryKey == null && sinkPrimaryKeys.size() == 1)
                || (primaryKey != null && !primaryKey.isPresent())) {
            int index = random.nextInt(blockingQueues.size());
            offerRowElement(index, element);
        } else if (primaryKey == null) {
            if (failurePolicy.continueOtherTables()) {
                handleTableFailure(
                        tableId,
                        MultiTableFailurePhase.RUNTIME_WRITE,
                        new IllegalStateException(
                                "No active sink writer found for table " + tableId));
                return;
            }
            throw new RuntimeException("multi table sink can not write table: " + tableId);
        } else {
            Object object = element.getField(primaryKey.get());
            int index = 0;
            if (object != null) {
                index = (object.hashCode() & Integer.MAX_VALUE) % blockingQueues.size();
            }
            offerRowElement(index, element);
        }
    }

    /**
     * Starts the queue workers exactly once before the first ordered request enters the queues.
     * Schema changes must share the same bootstrap path as row writes so the first DDL cannot
     * bypass the in-band barrier while another thread is still bringing workers online.
     */
    private synchronized void ensureQueueWorkersSubmitted() {
        if (submitted) {
            return;
        }
        runnable.forEach(executorService::submit);
        submitted = true;
    }

    /** Keeps queue insertion logic consistent for both row writes and schema-change barriers. */
    private void offerQueueElement(
            BlockingQueue<MultiTableWriterRunnable.QueueElement> queue,
            MultiTableWriterRunnable.QueueElement queueElement)
            throws IOException {
        try {
            while (!queue.offer(queueElement, 500, TimeUnit.MILLISECONDS)) {
                subSinkErrorCheck();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void offerRowElement(int index, SeaTunnelRow element) throws IOException {
        MultiTableWriterRunnable writerRunnable = runnable.get(index);
        MultiTableWriterRunnable.QueueElement queueElement =
                writerRunnable.countedRowRequest(element);
        try {
            offerQueueElement(blockingQueues.get(index), queueElement);
        } catch (IOException | RuntimeException error) {
            writerRunnable.cancelCountedRowRequest(queueElement);
            throw error;
        }
    }

    /**
     * Captures the state of all sub-writers for the given checkpoint.
     *
     * <p>First drains all blocking queues via {@link #checkQueueRemain()} to ensure all pending
     * rows have been written. Then acquires the lock on each {@link MultiTableWriterRunnable}
     * ({@code synchronized(runnable.get(i))}) to prevent concurrent mutation while each
     * sub-writer's state is being captured.
     *
     * @param checkpointId the checkpoint identifier
     * @return a list containing a single {@link MultiTableState} with all sub-writer states
     * @throws IOException if an error occurs during state capture
     */
    @Override
    public List<MultiTableState> snapshotState(long checkpointId) throws IOException {
        checkQueueRemain();
        subSinkErrorCheck();
        List<MultiTableState> multiTableStates = new ArrayList<>();
        Map<SinkIdentifier, List<?>> snapshotStates = new HashMap<>();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    new ArrayList<>(sinkWritersWithIndex.get(i).entrySet())) {
                synchronized (runnable.get(i)) {
                    try {
                        List states =
                                executeWithTableRetry(
                                        sinkWriterEntry.getKey().getTableIdentifier(),
                                        MultiTableFailurePhase.CHECKPOINT,
                                        () ->
                                                sinkWriterEntry
                                                        .getValue()
                                                        .snapshotState(checkpointId));
                        snapshotStates.put(sinkWriterEntry.getKey(), states);
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throwAsIOException(error);
                    } catch (Throwable error) {
                        if (failurePolicy.continueOtherTables()) {
                            handleTableFailure(
                                    sinkWriterEntry.getKey().getTableIdentifier(),
                                    MultiTableFailurePhase.CHECKPOINT,
                                    error);
                            continue;
                        }
                        throwAsIOException(error);
                    }
                }
            }
        }
        waitRuntimeTableFailuresHandled();
        subSinkErrorCheck();
        multiTableStates.add(
                new MultiTableState(snapshotStates, new ArrayList<>(failedTables.values())));
        return multiTableStates;
    }

    @Override
    public Optional<MultiTableCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    /**
     * Prepares commit info for all sub-writers in parallel.
     *
     * <p>After draining all queues via {@link #checkQueueRemain()} and checking for errors, submits
     * one task per queue to the executor service. Each task acquires the corresponding {@link
     * MultiTableWriterRunnable} lock and calls {@code prepareCommit} on every sub-writer assigned
     * to that queue. Results are aggregated into a single {@link MultiTableCommitInfo} using a
     * {@link ConcurrentHashMap}.
     *
     * <p>Blocks until all futures complete, then returns the aggregated commit info (or empty if no
     * sub-writer produced commit info).
     *
     * @param checkpointId the checkpoint identifier
     * @return commit info aggregated from all sub-writers, or empty if none
     * @throws IOException if a sub-writer fails during prepare commit
     */
    @Override
    public Optional<MultiTableCommitInfo> prepareCommit(long checkpointId) throws IOException {
        checkQueueRemain();
        subSinkErrorCheck();
        MultiTableCommitInfo multiTableCommitInfo =
                new MultiTableCommitInfo(new ConcurrentHashMap<>());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            int subWriterIndex = i;
            futures.add(
                    executorService.submit(
                            () -> {
                                synchronized (runnable.get(subWriterIndex)) {
                                    for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>
                                            sinkWriterEntry :
                                                    new ArrayList<>(
                                                            sinkWritersWithIndex
                                                                    .get(subWriterIndex)
                                                                    .entrySet())) {
                                        Optional<?> commit;
                                        try {
                                            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                                                    sinkWriterEntry.getValue();
                                            commit =
                                                    executeWithTableRetry(
                                                            sinkWriterEntry
                                                                    .getKey()
                                                                    .getTableIdentifier(),
                                                            MultiTableFailurePhase.CHECKPOINT,
                                                            () ->
                                                                    sinkWriter.prepareCommit(
                                                                            checkpointId));
                                        } catch (InterruptedException error) {
                                            Thread.currentThread().interrupt();
                                            throw new RuntimeException(error);
                                        } catch (Throwable error) {
                                            if (failurePolicy.continueOtherTables()) {
                                                handleTableFailure(
                                                        sinkWriterEntry
                                                                .getKey()
                                                                .getTableIdentifier(),
                                                        MultiTableFailurePhase.CHECKPOINT,
                                                        error);
                                                continue;
                                            }
                                            throw new RuntimeException(error);
                                        }
                                        commit.ifPresent(
                                                o ->
                                                        multiTableCommitInfo
                                                                .getCommitInfo()
                                                                .put(sinkWriterEntry.getKey(), o));
                                    }
                                }
                            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        waitRuntimeTableFailuresHandled();
        subSinkErrorCheck();
        if (multiTableCommitInfo.getCommitInfo().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(multiTableCommitInfo);
    }

    /**
     * Aborts the prepare phase for all sub-writers.
     *
     * <p>Drains remaining queues, then iterates over all sub-writers under their respective {@link
     * MultiTableWriterRunnable} locks, calling {@code abortPrepare()} on each. Uses a
     * first-exception-wins strategy: if multiple sub-writers throw, only the first exception is
     * propagated; subsequent errors are logged.
     */
    @Override
    public void abortPrepare() {
        Throwable firstE = null;
        try {
            checkQueueRemain();
        } catch (Exception e) {
            firstE = e;
        }
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                for (SinkWriter<SeaTunnelRow, ?, ?> sinkWriter :
                        sinkWritersWithIndex.get(i).values()) {
                    try {
                        sinkWriter.abortPrepare();
                    } catch (Throwable e) {
                        if (firstE == null) {
                            firstE = e;
                        }
                        log.error("abortPrepare error", e);
                    }
                }
            }
        }
        if (firstE != null) {
            throw new RuntimeException(firstE);
        }
    }

    /**
     * Closes this writer and releases all resources.
     *
     * <p>Drains remaining queues, then calls {@link ExecutorService#shutdownNow()} to interrupt all
     * consumer threads. Each sub-writer is closed under its respective {@link
     * MultiTableWriterRunnable} lock to avoid concurrent access. Finally, the shared {@link
     * MultiTableResourceManager} is closed and this method waits for the executor threads to stop.
     *
     * <p>Uses first-exception-wins error handling: if multiple sub-writers throw during close, only
     * the first exception is propagated. Resource manager close errors are logged but not
     * propagated.
     *
     * @throws RuntimeException wrapping the first {@link Throwable} caught from any sub-writer;
     *     note that the method signature declares {@code throws IOException} (inherited from the
     *     interface), but the current implementation always wraps in {@code RuntimeException}
     */
    @Override
    public void close() throws IOException {
        // The variables used in lambda expressions should be final or valid final, so they are
        // modified to arrays
        final Throwable[] firstE = {null};
        boolean failedTableReportOnly = false;
        try {
            checkQueueRemain();
        } catch (Exception e) {
            firstE[0] = e;
        }
        executorService.shutdownNow();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierSinkWriterMap =
                        sinkWritersWithIndex.get(i);
                sinkIdentifierSinkWriterMap.forEach(
                        (identifier, sinkWriter) -> {
                            try {
                                sinkWriter.close();
                            } catch (Throwable e) {
                                if (firstE[0] == null) {
                                    firstE[0] = e;
                                }
                                log.error("close error", e);
                            }
                        });
            }
        }
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        try {
            if (!executorService.awaitTermination(
                    EXECUTOR_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                RuntimeException timeoutException =
                        new RuntimeException(
                                String.format(
                                        "Timed out waiting %s seconds for multi-table sink writer threads to close",
                                        EXECUTOR_CLOSE_TIMEOUT_SECONDS));
                if (firstE[0] == null) {
                    firstE[0] = timeoutException;
                } else {
                    firstE[0].addSuppressed(timeoutException);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (firstE[0] == null) {
                firstE[0] = e;
            } else {
                firstE[0].addSuppressed(e);
            }
        }
        if (firstE[0] == null
                && failurePolicy.continueOtherTables()
                && jobMode == JobMode.BATCH
                && !failedTables.isEmpty()) {
            firstE[0] = getFirstFailureCause();
            failedTableReportOnly = true;
        }
        if (firstE[0] != null) {
            if (failurePolicy.continueOtherTables()
                    && jobMode == JobMode.BATCH
                    && !failedTables.isEmpty()) {
                boolean isolatedFailureReport =
                        failedTableReportOnly
                                || MultiTableFailureHelper.isIsolatedFailure(
                                        firstE[0].getMessage());
                throw new IOException(
                        isolatedFailureReport
                                ? buildIsolatedFailureSummary()
                                : buildFailureSummary(),
                        firstE[0]);
            }
            throw new RuntimeException(firstE[0]);
        }
    }

    /**
     * Aggregated flush triggered by the engine timer. Drains all blocking queues first, then calls
     * each sub-writer's flush action under the corresponding {@link MultiTableWriterRunnable} lock
     * to prevent concurrent writes.
     *
     * @param proxyContexts map from sink identifier to its {@link SinkContextProxy}
     */
    void aggregatedFlush(Map<SinkIdentifier, SinkContextProxy> proxyContexts) throws Exception {
        checkQueueRemain();
        subSinkErrorCheck();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                for (SinkIdentifier id : sinkWritersWithIndex.get(i).keySet()) {
                    SinkContextProxy proxy = proxyContexts.get(id);
                    if (proxy != null && proxy.getFlushAction() != null) {
                        try {
                            proxy.getFlushAction().run();
                        } catch (InterruptedException error) {
                            Thread.currentThread().interrupt();
                            throw error;
                        } catch (Exception error) {
                            if (failurePolicy.continueOtherTables()) {
                                handleTableFailure(
                                        id.getTableIdentifier(),
                                        MultiTableFailurePhase.TIMER_FLUSH,
                                        error);
                                continue;
                            }
                            throw error;
                        }
                    }
                }
            }
        }
    }

    /**
     * Busy-waits until all blocking queues are fully drained.
     *
     * <p>Polls each queue every 100 milliseconds, checking for sub-sink errors between iterations
     * via {@link #subSinkErrorCheck()}. This must complete before any lock-protected state
     * operations ({@link #snapshotState(long)}, {@link #prepareCommit(long)}) to ensure all
     * enqueued rows have been consumed by the writer threads.
     */
    private void checkQueueRemain() throws IOException {
        try {
            while (hasPendingRuntimeWrites()) {
                Thread.sleep(100);
                subSinkErrorCheck();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private boolean hasPendingRuntimeWrites() {
        for (BlockingQueue<MultiTableWriterRunnable.QueueElement> blockingQueue : blockingQueues) {
            if (!blockingQueue.isEmpty()) {
                return true;
            }
        }
        for (MultiTableWriterRunnable writerRunnable : runnable) {
            if (writerRunnable.hasPendingRowRequests()
                    || writerRunnable.isProcessingRow()
                    || writerRunnable.isHandlingTableFailure()) {
                return true;
            }
        }
        return false;
    }

    private void handleRuntimeTableFailure(String tableId, Throwable error) {
        if (failurePolicy.continueOtherTables()) {
            handleTableFailure(tableId, MultiTableFailurePhase.RUNTIME_WRITE, error);
            return;
        }
        fatalTableId = tableId;
        fatalThrowable = error;
    }

    private synchronized void handleTableFailure(
            String tableId, MultiTableFailurePhase phase, Throwable error) {
        if (containsFatalRowErrorHandlingFailure(error)) {
            fatalTableId = tableId;
            fatalThrowable = error;
            throw new RuntimeException(error);
        }
        if (tableId == null || tableId.trim().isEmpty()) {
            fatalTableId = tableId;
            fatalThrowable = error;
            return;
        }
        if (failedTables.containsKey(tableId)) {
            return;
        }
        MultiTableFailedTable failedTable =
                MultiTableFailureHelper.buildFailedTable(tableId, phase, "MultiTableSink", error);
        registerFailedTable(failedTable, true);
    }

    private <T> T executeWithTableRetry(
            String tableId, MultiTableFailurePhase phase, TableOperation<T> operation)
            throws Throwable {
        int retriedTimes = 0;
        while (true) {
            try {
                return operation.execute();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw error;
            } catch (Throwable error) {
                if (containsFatalRowErrorHandlingFailure(error)
                        || !failurePolicy.continueOtherTables()
                        || retriedTimes >= tableRetryTimes) {
                    throw error;
                }
                retriedTimes++;
                log.warn(
                        "Retry multi-table sink operation for table {} in phase {}, attempt {}/{}",
                        tableId,
                        phase,
                        retriedTimes,
                        tableRetryTimes,
                        error);
                waitBeforeTableRetry();
            }
        }
    }

    private boolean containsFatalRowErrorHandlingFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof RowErrorHandlingFatalException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void waitBeforeTableRetry() throws InterruptedException {
        if (tableRetryIntervalSeconds <= 0) {
            return;
        }
        try {
            TimeUnit.SECONDS.sleep(tableRetryIntervalSeconds);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw error;
        }
    }

    private void waitRuntimeTableFailuresHandled() {
        try {
            boolean handlingFailure = true;
            while (handlingFailure) {
                handlingFailure = false;
                for (MultiTableWriterRunnable writerRunnable : runnable) {
                    if (writerRunnable.isHandlingTableFailure()) {
                        handlingFailure = true;
                        break;
                    }
                }
                if (handlingFailure) {
                    Thread.sleep(10);
                }
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(error);
        }
    }

    private boolean isTableFailed(String tableId) {
        return tableId != null && failedTables.containsKey(tableId);
    }

    private Throwable getFirstFailureCause() {
        return failedTables.values().stream()
                .map(MultiTableFailedTable::getCause)
                .filter(cause -> cause != null)
                .findFirst()
                .orElseGet(
                        () ->
                                fatalThrowable != null
                                        ? fatalThrowable
                                        : new IllegalStateException(buildFailureSummary()));
    }

    private String buildFailureSummary() {
        List<MultiTableFailedTable> failures =
                failedTables.values().stream()
                        .sorted(
                                (left, right) ->
                                        left.getTablePath().compareTo(right.getTablePath()))
                        .collect(Collectors.toList());
        String title =
                fatalTableId == null
                        ? "Failed tables were isolated in multi-table sink."
                        : String.format(
                                "Failed table '%s' caused multi-table sink to abort.",
                                fatalTableId);
        return MultiTableFailureHelper.formatFailedTableSummary(title, failures);
    }

    private String buildIsolatedFailureSummary() {
        return MultiTableFailureHelper.withIsolatedFailureMarker(buildFailureSummary());
    }

    private void throwAsIOException(Throwable error) throws IOException {
        if (error instanceof IOException) {
            throw (IOException) error;
        }
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
        }
        throw new IOException(error);
    }

    private void registerInitialFailedTables(
            Collection<MultiTableFailedTable> initialFailedTables) {
        if (initialFailedTables == null || initialFailedTables.isEmpty()) {
            return;
        }
        initialFailedTables.forEach(failedTable -> registerFailedTable(failedTable, false));
    }

    private synchronized void registerFailedTable(
            MultiTableFailedTable failedTable, boolean printLog) {
        String tableId = failedTable.getTablePath();
        if (tableId == null || tableId.trim().isEmpty() || failedTables.containsKey(tableId)) {
            return;
        }
        failedTables.put(tableId, failedTable);
        if (printLog) {
            log.warn(
                    "Quarantine failed table in multi-table sink: {}",
                    MultiTableFailureHelper.formatFailedTableLine(failedTable),
                    failedTable.getCause());
        }
        removeTableWriters(tableId);
    }

    private void removeTableWriters(String tableId) {
        List<SinkIdentifier> sinkIdentifiers =
                sinkIdentifiersByTable.getOrDefault(tableId, Collections.emptyList());
        for (SinkIdentifier sinkIdentifier : sinkIdentifiers) {
            for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
                synchronized (runnable.get(i)) {
                    SinkWriter<SeaTunnelRow, ?, ?> writer =
                            sinkWritersWithIndex.get(i).get(sinkIdentifier);
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (Throwable closeError) {
                            log.warn(
                                    "Close quarantined writer failed for table {}, remove it anyway.",
                                    tableId,
                                    closeError);
                        }
                    }
                    sinkWritersWithIndex.get(i).remove(sinkIdentifier);
                    runnable.get(i).removeTableWriter(tableId);
                }
            }
            sinkWriters.remove(sinkIdentifier);
        }
        sinkPrimaryKeys.remove(tableId);
    }

    @FunctionalInterface
    private interface TableOperation<T> {
        T execute() throws Throwable;
    }

    /**
     * Carries deferred failure handling details so continue-other-tables mode can quarantine
     * failing sub-writers after the shared queue-freeze scope has been released.
     */
    private static class SchemaChangeFailure {

        private final String tableId;
        private final Throwable error;

        private SchemaChangeFailure(String tableId, Throwable error) {
            this.tableId = tableId;
            this.error = error;
        }

        public String getTableId() {
            return tableId;
        }

        public Throwable getError() {
            return error;
        }
    }
}

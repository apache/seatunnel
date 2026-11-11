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
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.MultiTableSinkReplicaMode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.HashUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
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
 * appropriate sub-writer. Routing follows the {@link MultiTableSinkReplicaMode} each writer
 * declares: a hash of the key for ordered delivery, or random selection when the writer allows it.
 * With several replicas, a writer that declares neither is rejected when this writer is created.
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

    /** Keeps the replica error readable for jobs with many tables. */
    private static final int MAX_TABLES_IN_REPLICA_ERROR = 10;

    private final Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters;
    private final Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext;
    private final ConcurrentMap<String, Optional<Integer>> sinkPrimaryKeys =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MultiTableSinkReplicaMode> sinkReplicaModes =
            new ConcurrentHashMap<>();
    /**
     * Whether this writer was built for one table. Like the single-writer fallback of {@link
     * MultiTableWriterRunnable}, a row whose table id does not name a table then belongs to it.
     */
    private final boolean singleTable;

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
        this(
                sinkWriters,
                queueSize,
                sinkWritersContext,
                failurePolicy,
                jobMode,
                initialFailedTables,
                tableRetryTimes,
                tableRetryIntervalSeconds,
                Collections.emptyMap());
    }

    /**
     * Creates the writer and checks the replica mode of every sub-writer before any queue or worker
     * exists. If the check or a later initialization step fails, the sub-writers are closed before
     * the failure is thrown.
     *
     * @param connectorNames connector name of each table identifier, used in error messages
     */
    MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext,
            MultiTableFailurePolicy failurePolicy,
            JobMode jobMode,
            Collection<MultiTableFailedTable> initialFailedTables,
            int tableRetryTimes,
            int tableRetryIntervalSeconds,
            Map<String, String> connectorNames) {
        try {
            validateReplicaRouting(sinkWriters, queueSize, connectorNames);
        } catch (RuntimeException e) {
            closeWritersAfterInitFailure(sinkWriters.values(), e);
            throw e;
        }
        this.singleTable =
                sinkWriters.keySet().stream()
                                .map(SinkIdentifier::getTableIdentifier)
                                .distinct()
                                .count()
                        == 1;
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
                            this.tableRetryIntervalSeconds,
                            queueSize > 1);
            runnable.add(r);
        }
        log.info("init multi table sink writer, queue size: {}", queueSize);
        try {
            initResourceManager(queueSize);
            registerInitialFailedTables(initialFailedTables);
        } catch (RuntimeException | Error e) {
            executorService.shutdownNow();
            closeWritersAfterInitFailure(sinkWriters.values(), e);
            if (resourceManager != null) {
                try {
                    resourceManager.close();
                } catch (Throwable closeError) {
                    e.addSuppressed(closeError);
                }
            }
            throw e;
        }
    }

    /**
     * Checks that the sub-writers can run with {@code replicaNum} writers of each table in one
     * subtask. With one replica every mode is accepted. With more, each table needs {@link
     * MultiTableSinkReplicaMode#KEYED} with a present, non-negative {@code primaryKey()}, or {@link
     * MultiTableSinkReplicaMode#RANDOM}, and all writers of a table must declare the same mode and,
     * for {@code KEYED}, the same key index.
     *
     * <p>It runs before any queue worker starts, for created and restored writers alike. A path
     * that adds writers after construction must call it for the new writers before it registers
     * them, so a table cannot start routing without a valid declaration.
     *
     * @param writers the sub-writers, keyed by table identifier and replica index
     * @param replicaNum the value of {@code multi_table_sink_replica}
     * @param connectorNames connector name of each table identifier, used in error messages
     * @throws IllegalArgumentException if a table cannot run with {@code replicaNum} replicas; the
     *     message names every such table, its connector and writer class
     */
    static void validateReplicaRouting(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers,
            int replicaNum,
            Map<String, String> connectorNames) {
        if (replicaNum <= 1) {
            return;
        }
        Map<String, List<SinkWriter<SeaTunnelRow, ?, ?>>> writersByTable = new TreeMap<>();
        writers.forEach(
                (identifier, writer) ->
                        writersByTable
                                .computeIfAbsent(
                                        identifier.getTableIdentifier(), key -> new ArrayList<>())
                                .add(writer));
        List<String> rejected = new ArrayList<>();
        for (Map.Entry<String, List<SinkWriter<SeaTunnelRow, ?, ?>>> table :
                writersByTable.entrySet()) {
            MultiTableSinkReplicaMode mode = null;
            Optional<Integer> key = Optional.empty();
            String problem = null;
            for (SinkWriter<SeaTunnelRow, ?, ?> writer : table.getValue()) {
                MultiTableSinkReplicaMode writerMode = replicaModeOf(writer);
                Optional<Integer> writerKey =
                        writerMode == MultiTableSinkReplicaMode.KEYED
                                ? primaryKeyOf(writer)
                                : Optional.empty();
                if (mode == null) {
                    mode = writerMode;
                    key = writerKey;
                } else if (mode != writerMode || !key.equals(writerKey)) {
                    problem =
                            String.format(
                                    "its writers do not agree: one declares %s, another %s",
                                    describeMode(mode, key), describeMode(writerMode, writerKey));
                    break;
                }
            }
            if (problem == null && mode == MultiTableSinkReplicaMode.UNSUPPORTED) {
                problem = "its writer declares neither KEYED nor RANDOM";
            } else if (problem == null
                    && mode == MultiTableSinkReplicaMode.KEYED
                    && (!key.isPresent() || key.get() < 0)) {
                problem =
                        "its writer declares KEYED, but primaryKey() returns "
                                + (key.isPresent() ? "index " + key.get() : "no key");
            }
            if (problem != null) {
                String connector = connectorNames.get(table.getKey());
                rejected.add(
                        String.format(
                                "table '%s' (%swriter %s): %s",
                                table.getKey(),
                                connector == null ? "" : "connector '" + connector + "', ",
                                table.getValue().get(0).getClass().getName(),
                                problem));
            }
        }
        if (!rejected.isEmpty()) {
            String option = SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key();
            String tables =
                    rejected.size() <= MAX_TABLES_IN_REPLICA_ERROR
                            ? String.join("; ", rejected)
                            : String.join("; ", rejected.subList(0, MAX_TABLES_IN_REPLICA_ERROR))
                                    + "; and "
                                    + (rejected.size() - MAX_TABLES_IN_REPLICA_ERROR)
                                    + " more tables";
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' is %d, so each table gets %d writers in one subtask, "
                                    + "but %s: %s. Set '%s' = 1. A connector supports more than "
                                    + "one writer per table only when its writer declares KEYED "
                                    + "with a valid primaryKey(), or RANDOM.",
                            option,
                            replicaNum,
                            replicaNum,
                            rejected.size() == 1
                                    ? "this table does not support that"
                                    : "these tables do not support that",
                            tables,
                            option));
        }
    }

    private static MultiTableSinkReplicaMode replicaModeOf(SinkWriter<SeaTunnelRow, ?, ?> writer) {
        if (!(writer instanceof SupportMultiTableSinkWriter)) {
            return MultiTableSinkReplicaMode.UNSUPPORTED;
        }
        MultiTableSinkReplicaMode mode = ((SupportMultiTableSinkWriter<?>) writer).replicaMode();
        return mode == null ? MultiTableSinkReplicaMode.UNSUPPORTED : mode;
    }

    private static Optional<Integer> primaryKeyOf(SinkWriter<SeaTunnelRow, ?, ?> writer) {
        Optional<Integer> key = ((SupportMultiTableSinkWriter<?>) writer).primaryKey();
        return key == null ? Optional.empty() : key;
    }

    private static String describeMode(MultiTableSinkReplicaMode mode, Optional<Integer> key) {
        return mode == MultiTableSinkReplicaMode.KEYED
                ? "KEYED with " + (key.isPresent() ? "key index " + key.get() : "no key")
                : mode.name();
    }

    /**
     * Closes sub-writers when the multi-table writer cannot be initialized, so that connections or
     * transactions they opened are released. Close failures are added to {@code failure} as
     * suppressed exceptions instead of hiding it.
     */
    static void closeWritersAfterInitFailure(
            Collection<SinkWriter<SeaTunnelRow, ?, ?>> writers, Throwable failure) {
        for (SinkWriter<SeaTunnelRow, ?, ?> writer : writers) {
            try {
                writer.close();
            } catch (Throwable closeError) {
                failure.addSuppressed(closeError);
            }
        }
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
     * populates the {@link #sinkPrimaryKeys} and {@link #sinkReplicaModes} maps used for row
     * routing in {@link #write(SeaTunnelRow)}. The declarations were checked by {@link
     * #validateReplicaRouting} before, so the writers of one table agree on what is cached.
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
                String tableId = entry.getKey().getTableIdentifier();
                sinkPrimaryKeys.put(tableId, primaryKeyOf(entry.getValue()));
                sinkReplicaModes.put(tableId, replicaModeOf(entry.getValue()));
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
     *   <li>The target table is the table the row's id names. In a writer built for one table, a
     *       row whose id is missing or names no table belongs to that table and uses its mode and
     *       key; a missing id never makes the row random.
     *   <li>With one replica, the row goes to queue 0.
     *   <li>For a {@link MultiTableSinkReplicaMode#KEYED} table, a row with a non-null key value is
     *       routed by {@link HashUtils#bucketIndex(int, int)} over {@code keyValue.hashCode()} and
     *       the queue count, so rows with the same key always go to the same queue in order. A row
     *       whose key value is {@code null} goes to queue 0.
     *   <li>For a {@link MultiTableSinkReplicaMode#RANDOM} table, the row goes to a random queue,
     *       even when the writer also has a key. Other modes are rejected when the writer is
     *       created.
     *   <li>A row whose table cannot be resolved fails the table under the failure policy, or
     *       throws a {@link RuntimeException}.
     * </ul>
     *
     * <p>Each accepted row is offered to exactly one queue.
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
        String targetTableId = resolveTargetTableId(tableId);
        MultiTableSinkReplicaMode replicaMode =
                targetTableId == null ? null : sinkReplicaModes.get(targetTableId);
        Optional<Integer> primaryKey =
                targetTableId == null ? null : sinkPrimaryKeys.get(targetTableId);
        if (replicaMode == null || primaryKey == null) {
            if (failurePolicy.continueOtherTables()) {
                handleTableFailure(
                        tableId,
                        MultiTableFailurePhase.RUNTIME_WRITE,
                        new IllegalStateException(
                                "No active sink writer found for table " + tableId));
                return;
            }
            throw new RuntimeException("multi table sink can not write table: " + tableId);
        }
        int index = selectQueueIndex(targetTableId, replicaMode, primaryKey, element);
        offerRowElement(index, element);
    }

    /**
     * Returns the table whose routing a row follows: the table its id names, or, in a writer built
     * for one table, that table when the id is missing or names no table. The queue worker falls
     * back to the same single writer, so such a row keeps the routing of the table it is written
     * to. Returns {@code null} when the table cannot be resolved.
     */
    private String resolveTargetTableId(String tableId) {
        if (tableId != null && sinkReplicaModes.containsKey(tableId)) {
            return tableId;
        }
        if (!singleTable || sinkReplicaModes.size() != 1) {
            return null;
        }
        Iterator<String> tables = sinkReplicaModes.keySet().iterator();
        return tables.hasNext() ? tables.next() : null;
    }

    /**
     * Selects the queue of a row: queue 0 with one replica, the hash of the key for {@link
     * MultiTableSinkReplicaMode#KEYED}, and a random queue for {@link
     * MultiTableSinkReplicaMode#RANDOM}, even when the writer also has a key. {@link
     * MultiTableSinkReplicaMode#UNSUPPORTED} with several replicas is rejected when the writer is
     * created, and again here.
     */
    private int selectQueueIndex(
            String tableId,
            MultiTableSinkReplicaMode replicaMode,
            Optional<Integer> primaryKey,
            SeaTunnelRow element) {
        int queueCount = blockingQueues.size();
        if (queueCount == 1) {
            return 0;
        }
        switch (replicaMode) {
            case KEYED:
                if (!primaryKey.isPresent()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Table '%s' declares KEYED, but has no key to route by.",
                                    tableId));
                }
                int keyIndex = primaryKey.get();
                if (keyIndex < 0 || keyIndex >= element.getArity()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Cannot route a row of table '%s' by key index %d: the row "
                                            + "has %d fields.",
                                    tableId, keyIndex, element.getArity()));
                }
                Object key = element.getField(keyIndex);
                return key == null ? 0 : HashUtils.bucketIndex(key.hashCode(), queueCount);
            case RANDOM:
                return random.nextInt(queueCount);
            default:
                throw new IllegalStateException(
                        String.format(
                                "Table '%s' declares %s, which does not support %d replicas.",
                                tableId, replicaMode, queueCount));
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
        sinkReplicaModes.remove(tableId);
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

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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.Unit;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Default error sink writer that routes error rows to a configured sink asynchronously. */
@Slf4j
public class DefaultErrorSinkWriter<T> implements ErrorSinkRowWriter<T> {

    private static final long serialVersionUID = 1L;
    private static final long DEFAULT_CLOSE_TIMEOUT_MILLIS = 60_000L;
    private static final long BLOCK_OFFER_RETRY_MILLIS = 100L;

    private final StageErrorConfig stageConfig;
    private final ErrorSinkConfig sinkConfig;
    private final long jobId;
    private final int subtaskIndex;
    private final ClassLoaderService classLoaderService;
    private final transient MetricsContext metricsContext;
    private final transient EventListener eventListener;

    private transient volatile boolean initialized;
    private transient volatile boolean closed;
    private transient volatile Throwable workerFailure;

    private transient SeaTunnelRowType errorRowType;
    private transient SeaTunnelSink<SeaTunnelRow, ?, ?, ?> errorSink;
    private transient SinkWriter<SeaTunnelRow, ?, ?> writer;
    private transient BlockingQueue<SeaTunnelRow> queue;
    private transient Thread workerThread;
    private transient ClassLoader errorSinkClassLoader;
    private transient List<URL> errorSinkPluginJars;
    private transient volatile boolean classLoaderReleased;
    private transient AtomicInteger pendingRows;
    private transient Object writerLock;
    private transient SimpleWriterContext writerContext;

    public DefaultErrorSinkWriter(
            StageErrorConfig stageConfig,
            ErrorSinkConfig sinkConfig,
            long jobId,
            int subtaskIndex,
            ClassLoaderService classLoaderService) {
        this(stageConfig, sinkConfig, jobId, subtaskIndex, classLoaderService, null, null);
    }

    public DefaultErrorSinkWriter(
            StageErrorConfig stageConfig,
            ErrorSinkConfig sinkConfig,
            long jobId,
            int subtaskIndex,
            ClassLoaderService classLoaderService,
            MetricsContext metricsContext,
            EventListener eventListener) {
        this.stageConfig = stageConfig;
        this.sinkConfig = sinkConfig;
        this.jobId = jobId;
        this.subtaskIndex = subtaskIndex;
        this.classLoaderService = classLoaderService;
        this.metricsContext = metricsContext == null ? new NoopMetricsContext() : metricsContext;
        this.eventListener = eventListener == null ? event -> {} : eventListener;
    }

    /** Initializes the child sink and worker thread during task startup. */
    public void open() {
        ensureInitialized();
    }

    @Override
    public void write(RowErrorContext ctx, T row, Throwable t) throws Exception {
        writeAndCheckAccepted(ctx, row, t);
    }

    @Override
    public boolean writeAndCheckAccepted(RowErrorContext ctx, T row, Throwable t) throws Exception {
        if (stageConfig.getMode() != ErrorHandlerMode.ROUTE) {
            return false;
        }
        ensureInitialized();

        if (workerFailure != null) {
            throw new RuntimeException(
                    String.format(
                            "Error sink previously failed for stage [%s], plugin [%s]",
                            ctx.getStage(), ctx.getPluginName()),
                    workerFailure);
        }

        SeaTunnelRow errorRow = buildErrorRow(ctx, row, t);

        try {
            switch (stageConfig.getQueueOverflowPolicy()) {
                case DROP:
                    pendingRows.incrementAndGet();
                    if (!queue.offer(errorRow)) {
                        pendingRows.decrementAndGet();
                        return false;
                    }
                    break;
                case BLOCK:
                    pendingRows.incrementAndGet();
                    enqueueWithBlockPolicy(ctx, errorRow);
                    break;
                case FAIL:
                default:
                    pendingRows.incrementAndGet();
                    if (!queue.offer(errorRow)) {
                        pendingRows.decrementAndGet();
                        throw new RuntimeException(
                                String.format(
                                        "Error queue overflow for stage [%s], plugin [%s]",
                                        ctx.getStage(), ctx.getPluginName()));
                    }
                    break;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingRows.decrementAndGet();
            throw new RuntimeException("Interrupted while enqueuing error row for error sink", e);
        }
        return true;
    }

    private void enqueueWithBlockPolicy(RowErrorContext ctx, SeaTunnelRow errorRow)
            throws Exception {
        boolean enqueued = false;
        try {
            while (true) {
                throwWorkerFailureIfAny();
                if (closed) {
                    throw new RuntimeException(
                            String.format(
                                    "Error sink is closing for stage [%s], plugin [%s]",
                                    ctx.getStage(), ctx.getPluginName()));
                }
                if (queue.offer(errorRow, BLOCK_OFFER_RETRY_MILLIS, TimeUnit.MILLISECONDS)) {
                    enqueued = true;
                    return;
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception | Error e) {
            if (!enqueued) {
                pendingRows.decrementAndGet();
            }
            throw e;
        }
    }

    @Override
    public void flush() throws Exception {
        flushInternal(null);
    }

    @Override
    public void flush(long checkpointId) throws Exception {
        flushInternal(checkpointId);
    }

    private void flushInternal(Long checkpointId) throws Exception {
        if (!initialized) {
            return;
        }
        waitForPendingRows(DEFAULT_CLOSE_TIMEOUT_MILLIS);
        throwWorkerFailureIfAny();

        SinkWriter<SeaTunnelRow, ?, ?> sinkWriter = this.writer;
        if (sinkWriter == null) {
            return;
        }
        synchronized (writerLock) {
            throwWorkerFailureIfAny();
            try {
                RunnableWithException flushAction = writerContext.getFlushAction();
                if (flushAction != null) {
                    flushAction.run();
                }
                if (checkpointId == null) {
                    sinkWriter.prepareCommit();
                } else {
                    sinkWriter.prepareCommit(checkpointId);
                }
            } catch (Throwable e) {
                workerFailure = e;
                if (e instanceof Exception) {
                    throw (Exception) e;
                }
                throw new RuntimeException(e);
            }
        }
        throwWorkerFailureIfAny();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        Thread currentWorkerThread = workerThread;
        if (currentWorkerThread != null) {
            currentWorkerThread.interrupt();
        }
        waitForWorkerTermination(DEFAULT_CLOSE_TIMEOUT_MILLIS);

        Throwable closeEx = null;
        try {
            if (currentWorkerThread != null && currentWorkerThread.isAlive()) {
                log.warn(
                        "Error sink worker thread is still alive after close timeout. jobId={}, pluginName={}, threadName={}. "
                                + "Will release the error sink classloader anyway to reduce classloader leak risk.",
                        jobId,
                        sinkConfig.getPluginName(),
                        currentWorkerThread.getName());
                closeEx =
                        new RuntimeException(
                                String.format(
                                        "Timed out waiting for error sink worker to close. jobId=%d, pluginName=%s",
                                        jobId, sinkConfig.getPluginName()));
            } else {
                // Worker thread stopped; close remaining resources.
                closeWriterIfPossible();
            }
        } catch (Throwable e) {
            closeEx = e;
            log.warn("Failed to close error sink writer", e);
        } finally {
            try {
                shutdownMySqlAbandonedConnectionCleanupThreadIfPresent();
            } catch (Throwable cleanupEx) {
                if (cleanupEx instanceof Error) {
                    throw (Error) cleanupEx;
                }
                log.warn(
                        "Failed to shutdown MySQL abandoned connection cleanup thread (if present). jobId={}, pluginName={}",
                        jobId,
                        sinkConfig.getPluginName(),
                        cleanupEx);
                if (closeEx != null) {
                    closeEx.addSuppressed(cleanupEx);
                }
            }
            releaseErrorSinkClassLoaderIfNeeded();
        }
        if (closeEx != null && workerFailure != null) {
            closeEx.addSuppressed(workerFailure);
        }
        if (closeEx != null) {
            if (closeEx instanceof Exception) {
                throw (Exception) closeEx;
            }
            throw new RuntimeException(closeEx);
        }
        throwWorkerFailureIfAny();
    }

    private synchronized void ensureInitialized() {
        if (initialized) {
            return;
        }
        try {
            log.info(
                    "Initializing DefaultErrorSinkWriter with mode={}, pluginName={}, errorTable={}, optionKeys={}",
                    stageConfig.getMode(),
                    sinkConfig.getPluginName(),
                    sinkConfig.getErrorTable(),
                    optionKeys(sinkConfig.getOptions()));
            this.errorRowType = buildErrorRowType();

            // Resolve connector jars for the error sink plugin once and let the shared
            // ClassLoaderService manage the actual classloader lifecycle, so that
            // any background threads (such as MySQL cleanup threads) can be properly
            // detached from this classloader when the job finishes.
            SeaTunnelSinkPluginDiscovery discovery = new SeaTunnelSinkPluginDiscovery();
            if (errorSinkClassLoader == null) {
                PluginIdentifier pluginIdentifier =
                        PluginIdentifier.of(
                                EngineType.SEATUNNEL.getEngine(),
                                PluginType.SINK.getType(),
                                sinkConfig.getPluginName());
                List<URL> pluginJars =
                        discovery.getPluginJarAndDependencyPaths(
                                Collections.singletonList(pluginIdentifier));
                this.errorSinkPluginJars = pluginJars;
                errorSinkClassLoader = classLoaderService.getClassLoader(jobId, pluginJars);
                log.info(
                        "Created dedicated error sink classloader via ClassLoaderService for pluginName={}, jars={}",
                        sinkConfig.getPluginName(),
                        pluginJars);
            }

            ClassLoader contextClassLoader = errorSinkClassLoader;
            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink;
            try {
                // Ensure that SPI / ServiceLoader usages inside the connector (such as
                // JdbcDialectLoader) see the dedicated error-sink classloader as the
                // thread context classloader. Otherwise dialect discovery may fail even
                // though the connector jars are available.
                Thread.currentThread().setContextClassLoader(contextClassLoader);

                TableSinkFactory<SeaTunnelRow, ?, ?, ?> tableSinkFactory =
                        FactoryUtil.discoverFactory(
                                contextClassLoader,
                                TableSinkFactory.class,
                                sinkConfig.getPluginName());

                Map<String, Object> connectorOptions = sinkConfig.getOptions();
                CatalogTable catalogTable = buildCatalogTable(errorRowType, connectorOptions);
                ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(connectorOptions);

                SeaTunnelSinkPluginDiscovery sinkDiscovery = new SeaTunnelSinkPluginDiscovery();
                sink =
                        FactoryUtil.createAndPrepareSink(
                                catalogTable,
                                readonlyConfig,
                                contextClassLoader,
                                sinkConfig.getPluginName(),
                                pluginIdentifier ->
                                        sinkDiscovery.createPluginInstance(
                                                PluginIdentifier.of(
                                                        EngineType.SEATUNNEL.getEngine(),
                                                        PluginType.SINK.getType(),
                                                        pluginIdentifier.getPluginName())),
                                tableSinkFactory);
            } finally {
                Thread.currentThread().setContextClassLoader(previousClassLoader);
            }

            this.errorSink = sink;
            validateSupportedErrorSinkLifecycle(sinkConfig.getPluginName(), sink);
            SimpleWriterContext writerContext =
                    new SimpleWriterContext(
                            metricsContextOrNoop(), eventListenerOrNoop(), subtaskIndex);
            this.writerContext = writerContext;
            @SuppressWarnings("unchecked")
            SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                    (SinkWriter<SeaTunnelRow, ?, ?>) sink.createWriter(writerContext);
            this.writer = sinkWriter;

            int capacity =
                    stageConfig.getQueueCapacity() > 0 ? stageConfig.getQueueCapacity() : 10000;
            this.queue = new ArrayBlockingQueue<>(capacity);
            this.pendingRows = new AtomicInteger(0);
            this.writerLock = new Object();
            this.closed = false;
            this.workerThread =
                    new Thread(
                            this::drainLoop, "seatunnel-error-sink-" + sinkConfig.getPluginName());
            this.workerThread.setDaemon(true);
            if (errorSinkClassLoader != null) {
                this.workerThread.setContextClassLoader(errorSinkClassLoader);
            }
            this.workerThread.start();
            initialized = true;
        } catch (Throwable e) {
            if (e instanceof Error) {
                throw (Error) e;
            }
            log.error(
                    "Failed to initialize error sink writer for pluginName={}, errorTable={}, optionKeys={}",
                    sinkConfig.getPluginName(),
                    sinkConfig.getErrorTable(),
                    optionKeys(sinkConfig.getOptions()),
                    e);
            try {
                closeWriterIfPossible();
            } catch (Throwable closeEx) {
                if (closeEx instanceof Error) {
                    throw (Error) closeEx;
                }
                e.addSuppressed(closeEx);
            }
            releaseErrorSinkClassLoaderIfNeeded();
            throw new SeaTunnelRuntimeException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Failed to initialize error sink writer",
                    e);
        }
    }

    private MetricsContext metricsContextOrNoop() {
        return metricsContext == null ? new NoopMetricsContext() : metricsContext;
    }

    private EventListener eventListenerOrNoop() {
        return eventListener == null ? event -> {} : eventListener;
    }

    static void validateSupportedErrorSinkLifecycle(
            String pluginName, SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink) throws Exception {
        List<String> unsupportedFeatures = new ArrayList<>();
        if (sink.getWriterStateSerializer().isPresent()) {
            unsupportedFeatures.add("writer state serializer");
        }
        if (sink.getCommitInfoSerializer().isPresent()) {
            unsupportedFeatures.add("commit info serializer");
        }
        if (sink.getAggregatedCommitInfoSerializer().isPresent()) {
            unsupportedFeatures.add("aggregated commit info serializer");
        }
        if (sink.createCommitter().isPresent()) {
            unsupportedFeatures.add("committer");
        }

        Optional<? extends SinkAggregatedCommitter<?, ?>> aggregatedCommitter =
                sink.createAggregatedCommitter();
        if (aggregatedCommitter.isPresent()) {
            unsupportedFeatures.add("aggregated committer");
            aggregatedCommitter.get().close();
        }

        if (!unsupportedFeatures.isEmpty()) {
            throw new SeaTunnelRuntimeException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    String.format(
                            "ROUTE error sink currently supports only sinks that do not require "
                                    + "writer state or committer lifecycle. pluginName=%s, unsupported=%s. "
                                    + "Disable transactional/exactly-once options for the error sink or use a simple error sink.",
                            pluginName, unsupportedFeatures));
        }
    }

    private void drainLoop() {
        final SinkWriter<SeaTunnelRow, ?, ?> sinkWriter = this.writer;
        try {
            while (!closed || !queue.isEmpty()) {
                SeaTunnelRow row;
                try {
                    row = queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    // Close path may interrupt the worker to speed up shutdown; this should not be
                    // treated as a sink failure. If close wasn't requested, preserve the interrupt
                    // status and treat it as a worker failure.
                    if (!closed) {
                        Thread.currentThread().interrupt();
                        workerFailure = ie;
                        log.error("Error sink worker thread interrupted unexpectedly", ie);
                        return;
                    }
                    if (queue.isEmpty()) {
                        break;
                    }
                    continue;
                }
                if (row == null) {
                    if (closed && queue.isEmpty()) {
                        break;
                    }
                    continue;
                }
                log.debug("Writing error row to sink: {}", row);
                try {
                    synchronized (writerLock) {
                        sinkWriter.write(row);
                    }
                } catch (Throwable writeEx) {
                    workerFailure = writeEx;
                    throw writeEx;
                } finally {
                    pendingRows.decrementAndGet();
                }
            }
        } catch (Throwable e) {
            if (e instanceof Error) {
                workerFailure = e;
                throw (Error) e;
            }
            workerFailure = e;
            if (closed) {
                log.warn("Error sink writer failed during shutdown", e);
            } else {
                log.error("Error sink writer failed", e);
            }
        } finally {
            try {
                if (sinkWriter != null) {
                    sinkWriter.close();
                }
            } catch (Throwable closeEx) {
                if (closeEx instanceof Error) {
                    throw (Error) closeEx;
                }
                if (workerFailure != null) {
                    workerFailure.addSuppressed(closeEx);
                } else {
                    workerFailure = closeEx;
                }
                log.warn("Failed to close error sink writer", closeEx);
            } finally {
                this.writer = null;
            }
        }
    }

    private void shutdownMySqlAbandonedConnectionCleanupThreadIfPresent() {
        ClassLoader classLoader = this.errorSinkClassLoader;
        if (classLoader == null) {
            return;
        }

        Class<?> cleanupClass;
        try {
            cleanupClass =
                    Class.forName(
                            "com.mysql.cj.jdbc.AbandonedConnectionCleanupThread",
                            false,
                            classLoader);
        } catch (ClassNotFoundException e) {
            return;
        }

        // Different MySQL connector/J versions expose different shutdown methods.
        if (invokeStaticNoArgIfExists(cleanupClass, "checkedShutdown")) {
            return;
        }
        if (invokeStaticNoArgIfExists(cleanupClass, "uncheckedShutdown")) {
            return;
        }
        invokeStaticNoArgIfExists(cleanupClass, "shutdown");
    }

    private boolean invokeStaticNoArgIfExists(Class<?> clazz, String methodName) {
        Method method;
        try {
            method = clazz.getDeclaredMethod(methodName);
        } catch (NoSuchMethodException e) {
            return false;
        }
        try {
            method.setAccessible(true);
            method.invoke(null);
            return true;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                    "Failed to invoke " + clazz.getName() + "." + methodName + "()", e);
        }
    }

    private void waitForWorkerTermination(long timeoutMillis) {
        if (workerThread == null) {
            return;
        }
        if (!workerThread.isAlive()) {
            return;
        }
        try {
            workerThread.join(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for error sink worker to close");
        }

        if (workerThread.isAlive()) {
            log.warn(
                    "Error sink worker thread did not terminate within {} ms, interrupting it",
                    timeoutMillis);
            workerThread.interrupt();
            try {
                workerThread.join(Math.min(5_000L, timeoutMillis));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn(
                        "Interrupted while waiting for error sink worker to close after interrupt");
            }
        }
    }

    private void waitForPendingRows(long timeoutMillis) throws Exception {
        AtomicInteger currentPendingRows = this.pendingRows;
        if (currentPendingRows == null) {
            return;
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (currentPendingRows.get() > 0) {
            throwWorkerFailureIfAny();
            if (System.currentTimeMillis() >= deadline) {
                throw new RuntimeException(
                        String.format(
                                "Timed out waiting for error sink queue to drain. jobId=%d, pluginName=%s, pendingRows=%d",
                                jobId, sinkConfig.getPluginName(), currentPendingRows.get()));
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for error sink queue", e);
            }
        }
    }

    private void throwWorkerFailureIfAny() throws Exception {
        Throwable failure = workerFailure;
        if (failure == null) {
            return;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        throw new RuntimeException(failure);
    }

    private void closeWriterIfPossible() {
        SinkWriter<SeaTunnelRow, ?, ?> sinkWriter = this.writer;
        if (sinkWriter == null) {
            return;
        }
        try {
            sinkWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close error sink writer", e);
        } finally {
            this.writer = null;
        }
    }

    private void releaseErrorSinkClassLoaderIfNeeded() {
        if (classLoaderReleased) {
            return;
        }
        List<URL> jars = this.errorSinkPluginJars;
        if (jars == null || jars.isEmpty()) {
            classLoaderReleased = true;
            return;
        }
        try {
            classLoaderService.releaseClassLoader(jobId, jars);
        } catch (Throwable e) {
            if (e instanceof Error) {
                throw (Error) e;
            }
            log.warn(
                    "Failed to release error sink classloader for jobId={}, pluginName={}, jars={}",
                    jobId,
                    sinkConfig.getPluginName(),
                    jars,
                    e);
        } finally {
            classLoaderReleased = true;
            errorSinkPluginJars = null;
            errorSinkClassLoader = null;
        }
    }

    private SeaTunnelRowType buildErrorRowType() {
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();

        fieldNames.add("error_stage");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("plugin_type");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("plugin_name");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("source_table_path");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("job_id");
        fieldTypes.add(BasicType.LONG_TYPE);
        fieldNames.add("error_message");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("exception_class");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("stacktrace");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("original_data");
        fieldTypes.add(BasicType.STRING_TYPE);
        fieldNames.add("occur_time");
        fieldTypes.add(LocalTimeType.LOCAL_DATE_TIME_TYPE);

        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private CatalogTable buildCatalogTable(
            SeaTunnelRowType rowType, Map<String, Object> optionsMap) {
        List<PhysicalColumn> columns = new ArrayList<>();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            columns.add(
                    PhysicalColumn.of(
                            rowType.getFieldName(i),
                            rowType.getFieldType(i),
                            (Long) null,
                            true,
                            null,
                            null));
        }
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(new ArrayList<>(columns))
                        .primaryKey(null)
                        .constraintKey(new ArrayList<>())
                        .build();

        String tableName =
                sinkConfig.getErrorTable() == null ? "st_error_data" : sinkConfig.getErrorTable();
        TableIdentifier tableId = TableIdentifier.of("error_sink", null, tableName);

        Map<String, String> options = new HashMap<>();
        optionsMap.forEach((k, v) -> options.put(k, v == null ? null : String.valueOf(v)));

        return CatalogTable.of(tableId, tableSchema, options, new ArrayList<>(), null);
    }

    private SeaTunnelRow buildErrorRow(RowErrorContext ctx, T row, Throwable t) {
        Object[] fields = new Object[errorRowType.getTotalFields()];
        int idx = 0;
        fields[idx++] = ctx.getStage();
        fields[idx++] = ctx.getPluginType();
        fields[idx++] = ctx.getPluginName();
        fields[idx++] = ctx.getTableId();
        fields[idx++] = jobId;
        fields[idx++] = t == null ? null : truncate(t.getMessage(), 1024);
        fields[idx++] = t == null ? null : t.getClass().getName();
        fields[idx++] =
                stageConfig.isIncludeStacktrace() && t != null
                        ? truncate(ExceptionUtils.getMessage(t), 4096)
                        : null;
        fields[idx++] =
                stageConfig.isIncludeOriginalData()
                        ? truncate(String.valueOf(row), stageConfig.getOriginalDataMaxLength())
                        : null;
        fields[idx] = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);

        return new SeaTunnelRow(fields);
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private static Set<String> optionKeys(Map<String, Object> options) {
        if (options == null || options.isEmpty()) {
            return Collections.emptySet();
        }
        return new TreeSet<>(options.keySet());
    }

    private static final class SimpleWriterContext implements SinkWriter.Context {
        private static final long serialVersionUID = 1L;

        private final MetricsContext metricsContext;
        private final EventListener eventListener;
        private final int subtaskIndex;
        private transient volatile RunnableWithException flushAction;

        private SimpleWriterContext(
                MetricsContext metricsContext, EventListener eventListener, int subtaskIndex) {
            this.metricsContext = metricsContext;
            this.eventListener = eventListener;
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        public int getIndexOfSubtask() {
            return subtaskIndex;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return eventListener;
        }

        @Override
        public void registerFlushAction(RunnableWithException action) {
            this.flushAction = java.util.Objects.requireNonNull(action, "flushAction");
        }

        @Override
        public RunnableWithException getFlushAction() {
            return flushAction;
        }
    }

    private static final class NoopMetricsContext implements MetricsContext {

        @Override
        public Counter counter(String name) {
            return new NoopCounter(name);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public Meter meter(String name) {
            return new NoopMeter(name);
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }
    }

    private static final class NoopCounter implements Counter {

        private final String name;
        private long count;

        private NoopCounter() {
            this("noop_counter");
        }

        private NoopCounter(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return Unit.COUNT;
        }

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public void set(long n) {
            count = n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    private static final class NoopMeter implements Meter {

        private final String name;
        private long count;

        private NoopMeter() {
            this("noop_meter");
        }

        private NoopMeter(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return Unit.COUNT;
        }

        @Override
        public void markEvent() {
            count++;
        }

        @Override
        public void markEvent(long n) {
            count += n;
        }

        @Override
        public double getRate() {
            return 0.0d;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}

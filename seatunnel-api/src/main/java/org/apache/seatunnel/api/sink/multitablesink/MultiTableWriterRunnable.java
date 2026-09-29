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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Consumes ordered queue requests for one sink queue. Both row writes and schema-change barriers
 * flow through this runnable so the worker always drains older rows before it switches any shared
 * sink schema.
 */
@Slf4j
public class MultiTableWriterRunnable implements Runnable {

    /** Writers that belong to this queue, keyed by logical source-table identifier. */
    private final Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap;
    /** Ordered requests for this queue: data rows or schema-change barrier markers. */
    private final BlockingQueue<QueueElement> queue;
    /** Preserves the historical sole-writer fallback for single-table jobs. */
    private final boolean allowSingleWriterFallback;
    /** Whether table failures should quarantine one table and keep other tables running. */
    private final boolean continueOnTableFailure;
    /** Reports per-table write failures back to the multi-table coordinator. */
    private final BiConsumer<String, Throwable> failureHandler;
    /** Maximum per-row retry count before this queue gives up on the current table. */
    private final int tableRetryTimes;
    /** Sleep interval between write retries for continue-other-tables mode. */
    private final int tableRetryIntervalSeconds;
    /** First fatal worker failure surfaced back to the coordinator. */
    private volatile Throwable throwable;
    /** Table currently being written, used in fail-fast diagnostics. */
    private volatile String currentTableId;
    /** Handles row-level write errors before the worker escalates to table failure. */
    private volatile MultiTableRowErrorHandler rowErrorHandler;
    /** Invoked after rows are successfully persisted and no collected-row error consumed them. */
    private volatile Consumer<SeaTunnelRow> writeSuccessHandler = row -> {};
    /** Marks that this worker is actively writing a data row. */
    private volatile boolean processingRow;
    /** Marks that this worker is still inside the failure-handler callback. */
    private volatile boolean handlingTableFailure;
    /** Counts queued or dequeued row requests until their write path has fully finished. */
    private final AtomicInteger pendingRowRequests = new AtomicInteger();

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<QueueElement> queue) {
        this(tableIdWriterMap, queue, false, (tableId, error) -> {});
    }

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<QueueElement> queue,
            boolean continueOnTableFailure,
            BiConsumer<String, Throwable> failureHandler) {
        this(tableIdWriterMap, queue, continueOnTableFailure, failureHandler, 0, 0);
    }

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<QueueElement> queue,
            boolean continueOnTableFailure,
            BiConsumer<String, Throwable> failureHandler,
            int tableRetryTimes,
            int tableRetryIntervalSeconds) {
        this.tableIdWriterMap = tableIdWriterMap;
        this.queue = queue;
        this.allowSingleWriterFallback = tableIdWriterMap.size() == 1;
        this.continueOnTableFailure = continueOnTableFailure;
        this.failureHandler = failureHandler;
        this.tableRetryTimes = Math.max(0, tableRetryTimes);
        this.tableRetryIntervalSeconds = Math.max(0, tableRetryIntervalSeconds);
    }

    public void setRowErrorHandler(MultiTableRowErrorHandler rowErrorHandler) {
        this.rowErrorHandler = rowErrorHandler;
    }

    public void setWriteSuccessHandler(Consumer<SeaTunnelRow> writeSuccessHandler) {
        this.writeSuccessHandler = writeSuccessHandler == null ? row -> {} : writeSuccessHandler;
    }

    @Override
    public void run() {
        while (true) {
            QueueElement queueElement = null;
            TableFailure tableFailure = null;
            try {
                queueElement = queue.poll(100, TimeUnit.MILLISECONDS);
                if (queueElement == null) {
                    continue;
                }
                processingRow = queueElement.isRowRequest();
                synchronized (this) {
                    queueElement.process(this);
                }
                processingRow = false;
            } catch (InterruptedException interruptedException) {
                processingRow = false;
                throwable = interruptedException;
                failPendingSchemaChangeRequests(queueElement, interruptedException);
                break;
            } catch (Throwable error) {
                if (queueElement instanceof RowWriteRequest) {
                    if (throwable == error) {
                        failPendingSchemaChangeRequests(queueElement, error);
                    } else {
                        tableFailure =
                                handleWriteFailure(((RowWriteRequest) queueElement).row, error);
                        if (tableFailure != null) {
                            if (notifyTableFailure(tableFailure)) {
                                continue;
                            }
                            failPendingSchemaChangeRequests(
                                    queueElement, throwable != null ? throwable : error);
                        } else {
                            failPendingSchemaChangeRequests(queueElement, error);
                        }
                    }
                } else {
                    log.error(
                            String.format(
                                    "MultiTableWriterRunnable error when process queue element %s",
                                    queueElement),
                            error);
                    throwable = error;
                    failPendingSchemaChangeRequests(queueElement, error);
                }
                processingRow = false;
                break;
            } finally {
                if (queueElement != null && queueElement.isCountedRowRequest()) {
                    pendingRowRequests.decrementAndGet();
                }
            }
        }
    }

    /**
     * Releases any queued schema-change barriers when this worker dies before reaching them. That
     * keeps applySchemaChange on the fail-fast path instead of waiting forever behind a dead queue.
     */
    private void failPendingSchemaChangeRequests(QueueElement currentElement, Throwable failure) {
        if (currentElement != null) {
            currentElement.fail(failure);
        }
        for (QueueElement pendingElement : queue) {
            pendingElement.fail(failure);
        }
    }

    /**
     * Applies one queued row write while the runnable monitor is already held by the worker.
     * Schema-change barriers reuse the same monitor, so older rows are always drained first.
     */
    void writeRow(SeaTunnelRow row) throws Throwable {
        if (row.getArity() == 0) {
            log.debug(
                    "Skip control SeaTunnelRow with zero arity in MultiTableWriterRunnable: {}",
                    row);
            return;
        }
        SinkWriter<SeaTunnelRow, ?, ?> writer = tableIdWriterMap.get(row.getTableId());
        if (writer == null) {
            if (allowSingleWriterFallback && tableIdWriterMap.size() == 1) {
                writer = tableIdWriterMap.values().stream().findFirst().get();
                currentTableId = tableIdWriterMap.keySet().stream().findFirst().get();
            } else if (continueOnTableFailure) {
                log.debug("Skip row for quarantined table {}", row.getTableId());
                return;
            } else {
                currentTableId = row.getTableId();
                throw new RuntimeException(
                        "MultiTableWriterRunnable can't find writer for tableId: "
                                + row.getTableId());
            }
        } else {
            currentTableId = row.getTableId();
        }
        try {
            beginCollectedRowErrorOutcomeProbe(row);
            writeWithRetry(writer, row, currentTableId);
            if (!consumeCollectedRowErrorOutcome(row)) {
                writeSuccessHandler.accept(row);
            }
        } catch (InterruptedException interruptedException) {
            clearCollectedRowErrorOutcomeProbe(row);
            throw interruptedException;
        } catch (Throwable error) {
            clearCollectedRowErrorOutcomeProbe(row);
            try {
                if (tryHandleRowError(writer, row, error)) {
                    return;
                }
            } catch (Throwable handlerException) {
                throwable = handlerException;
                throw handlerException;
            }
            throw error;
        }
    }

    /**
     * Parks this queue worker at the shared schema-change barrier. The last arriving worker runs
     * the actual schema mutation while the others wait behind the same completion latch.
     */
    void awaitSchemaChangeBarrier(SchemaChangeBarrier schemaChangeBarrier) throws IOException {
        schemaChangeBarrier.reachBarrier();
    }

    private boolean tryHandleRowError(
            SinkWriter<SeaTunnelRow, ?, ?> writer, SeaTunnelRow row, Throwable error)
            throws Throwable {
        if (containsFatalRowErrorHandlingFailure(error)) {
            throw error;
        }
        if (row == null || rowErrorHandler == null || writer == null) {
            return false;
        }
        try {
            boolean handled = rowErrorHandler.handleRowError(writer, currentTableId, row, error);
            if (handled) {
                return true;
            }
            return false;
        } catch (Throwable handlerException) {
            log.error(
                    String.format("RowErrorHandler threw exception when handling row %s", row),
                    handlerException);
            handlerException.addSuppressed(error);
            throw handlerException;
        }
    }

    private TableFailure handleWriteFailure(SeaTunnelRow row, Throwable error) {
        log.error(String.format("MultiTableWriterRunnable error when write row %s", row), error);
        if (containsFatalRowErrorHandlingFailure(error)) {
            throwable = error;
            return null;
        }
        String failedTableId =
                currentTableId != null ? currentTableId : row == null ? null : row.getTableId();
        if (continueOnTableFailure && failedTableId != null && !failedTableId.trim().isEmpty()) {
            removeTableWriter(failedTableId);
            currentTableId = null;
            handlingTableFailure = true;
            return new TableFailure(failedTableId, error);
        }
        throwable = error;
        return null;
    }

    private boolean consumeCollectedRowErrorOutcome(SeaTunnelRow row) {
        return rowErrorHandler != null && rowErrorHandler.consumeCollectedRowErrorOutcome(row);
    }

    private void beginCollectedRowErrorOutcomeProbe(SeaTunnelRow row) {
        if (rowErrorHandler != null) {
            rowErrorHandler.beginCollectedRowErrorOutcomeProbe(row);
        }
    }

    private void clearCollectedRowErrorOutcomeProbe(SeaTunnelRow row) {
        if (rowErrorHandler != null) {
            rowErrorHandler.clearCollectedRowErrorOutcomeProbe(row);
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

    private boolean notifyTableFailure(TableFailure tableFailure) {
        try {
            failureHandler.accept(tableFailure.tableId, tableFailure.error);
            return true;
        } catch (Throwable error) {
            throwable = error;
            return false;
        } finally {
            handlingTableFailure = false;
            processingRow = false;
        }
    }

    private void writeWithRetry(
            SinkWriter<SeaTunnelRow, ?, ?> writer, SeaTunnelRow row, String tableId)
            throws Throwable {
        int retriedTimes = 0;
        while (true) {
            try {
                writer.write(row);
                return;
            } catch (Throwable error) {
                if (!continueOnTableFailure || retriedTimes >= tableRetryTimes) {
                    throw error;
                }
                retriedTimes++;
                log.warn(
                        "Retry multi-table sink write for table {}, attempt {}/{}",
                        tableId,
                        retriedTimes,
                        tableRetryTimes,
                        error);
                waitBeforeRetry();
            }
        }
    }

    private void waitBeforeRetry() throws InterruptedException {
        if (tableRetryIntervalSeconds <= 0) {
            return;
        }
        try {
            TimeUnit.SECONDS.sleep(tableRetryIntervalSeconds);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw interruptedException;
        }
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getCurrentTableId() {
        return currentTableId;
    }

    public boolean isProcessingRow() {
        return processingRow;
    }

    public boolean isHandlingTableFailure() {
        return handlingTableFailure;
    }

    public boolean hasPendingRowRequests() {
        return pendingRowRequests.get() > 0;
    }

    QueueElement countedRowRequest(SeaTunnelRow row) {
        pendingRowRequests.incrementAndGet();
        return new RowWriteRequest(row, true);
    }

    void cancelCountedRowRequest(QueueElement queueElement) {
        if (queueElement.isCountedRowRequest()) {
            pendingRowRequests.decrementAndGet();
        }
    }

    public synchronized void removeTableWriter(String tableId) {
        tableIdWriterMap.remove(tableId);
    }

    /** Creates one ordered queue element that writes a data row. */
    static QueueElement rowRequest(SeaTunnelRow row) {
        return new RowWriteRequest(row, false);
    }

    /** Creates one ordered queue element that blocks on the shared schema-change barrier. */
    static QueueElement schemaChangeRequest(SchemaChangeBarrier schemaChangeBarrier) {
        return new SchemaChangeRequest(schemaChangeBarrier);
    }

    /** Represents one ordered queue action: either a row write or a schema-change barrier. */
    interface QueueElement {

        void process(MultiTableWriterRunnable runnable) throws Throwable;

        /** Allows worker shutdown paths to fail pending queue elements without processing them. */
        default void fail(Throwable failure) {}

        /** Distinguishes queued row writes from schema-maintenance requests. */
        default boolean isRowRequest() {
            return false;
        }

        /** Whether this row request was counted by {@link #pendingRowRequests}. */
        default boolean isCountedRowRequest() {
            return false;
        }
    }

    private static class RowWriteRequest implements QueueElement {

        private final SeaTunnelRow row;
        private final boolean counted;

        private RowWriteRequest(SeaTunnelRow row, boolean counted) {
            this.row = row;
            this.counted = counted;
        }

        @Override
        public void process(MultiTableWriterRunnable runnable) throws Throwable {
            runnable.writeRow(row);
        }

        @Override
        public boolean isRowRequest() {
            return true;
        }

        @Override
        public boolean isCountedRowRequest() {
            return counted;
        }

        @Override
        public String toString() {
            return "row[" + row + "]";
        }
    }

    private static class SchemaChangeRequest implements QueueElement {

        private final SchemaChangeBarrier schemaChangeBarrier;

        private SchemaChangeRequest(SchemaChangeBarrier schemaChangeBarrier) {
            this.schemaChangeBarrier = schemaChangeBarrier;
        }

        @Override
        public void process(MultiTableWriterRunnable runnable) throws IOException {
            runnable.awaitSchemaChangeBarrier(schemaChangeBarrier);
        }

        @Override
        public void fail(Throwable failure) {
            schemaChangeBarrier.fail(failure);
        }

        @Override
        public String toString() {
            return "schema-change[" + schemaChangeBarrier.getTablePath() + "]";
        }
    }

    private static class TableFailure {
        private final String tableId;
        private final Throwable error;

        private TableFailure(String tableId, Throwable error) {
            this.tableId = tableId;
            this.error = error;
        }
    }
}

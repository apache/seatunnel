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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Consumes ordered queue requests for one sink queue.
 *
 * <p>Both row writes and schema-change barriers flow through this runnable so the worker always
 * drains older rows before it switches any shared sink schema. The parent {@link
 * MultiTableSinkWriter} synchronizes on this runnable before lifecycle operations that interact
 * with sub-writers. Holding the same monitor during row writes keeps those lifecycle operations
 * from racing with an active write.
 *
 * <p>When table-level failure isolation is enabled, write failures are reported through the
 * configured handler and the failed table writer is removed so other tables can continue.
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
    /** Marks that this worker is actively writing a data row. */
    private volatile boolean processingRow;
    /** Marks that this worker is still inside the failure-handler callback. */
    private volatile boolean handlingTableFailure;

    /**
     * Creates a worker that stops on the first table write failure.
     *
     * @param tableIdWriterMap writers keyed by table identifier
     * @param queue row queue owned by this runnable
     */
    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<QueueElement> queue) {
        this(tableIdWriterMap, queue, false, (tableId, error) -> {});
    }

    /**
     * Creates a worker with optional table-level failure isolation and no retry.
     *
     * @param tableIdWriterMap writers keyed by table identifier
     * @param queue row queue owned by this runnable
     * @param continueOnTableFailure whether a failed table should be isolated instead of stopping
     *     the worker
     * @param failureHandler callback invoked after a table is isolated
     */
    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<QueueElement> queue,
            boolean continueOnTableFailure,
            BiConsumer<String, Throwable> failureHandler) {
        this(tableIdWriterMap, queue, continueOnTableFailure, failureHandler, 0, 0);
    }

    /**
     * Creates a worker with optional table-level failure isolation and bounded write retries.
     *
     * @param tableIdWriterMap writers keyed by table identifier
     * @param queue row queue owned by this runnable
     * @param continueOnTableFailure whether a failed table should be isolated instead of stopping
     *     the worker
     * @param failureHandler callback invoked after a table is isolated
     * @param tableRetryTimes maximum retry attempts before a table is treated as failed
     * @param tableRetryIntervalSeconds seconds to wait between retry attempts
     */
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

    /**
     * Runs the queue-draining loop until interrupted or an unrecoverable write failure is captured.
     *
     * <p>Rows with zero arity are control signals for schema evolution. Real data rows are written
     * while holding this runnable's monitor so parent lifecycle operations can acquire the same
     * lock before interacting with sub-writers.
     */
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
                    tableFailure = handleWriteFailure(((RowWriteRequest) queueElement).row, error);
                    if (tableFailure != null) {
                        if (notifyTableFailure(tableFailure)) {
                            continue;
                        }
                        failPendingSchemaChangeRequests(
                                queueElement, throwable != null ? throwable : error);
                    } else {
                        failPendingSchemaChangeRequests(queueElement, error);
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
                throw new RuntimeException(
                        "MultiTableWriterRunnable can't find writer for tableId: "
                                + row.getTableId());
            }
        } else {
            currentTableId = row.getTableId();
        }
        writeWithRetry(writer, row, currentTableId);
    }

    /**
     * Parks this queue worker at the shared schema-change barrier. The last arriving worker runs
     * the actual schema mutation while the others wait behind the same completion latch.
     */
    void awaitSchemaChangeBarrier(SchemaChangeBarrier schemaChangeBarrier) throws IOException {
        schemaChangeBarrier.reachBarrier();
    }

    /**
     * Converts a write failure into either a terminal worker error or an isolated table failure.
     */
    private TableFailure handleWriteFailure(SeaTunnelRow row, Throwable error) {
        log.error(String.format("MultiTableWriterRunnable error when write row %s", row), error);
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

    /**
     * Notifies the parent writer about an isolated table and clears row-processing state.
     *
     * @param tableFailure failed table context
     * @return {@code true} when the parent handler accepts the isolated table failure
     */
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

    /**
     * Writes one row and retries only when table-level failure isolation is enabled.
     *
     * @param writer target sub-writer
     * @param row row to write
     * @param tableId table identifier used in retry logs
     * @throws Throwable when the write still fails after all allowed retries
     */
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

    /**
     * Waits between retry attempts and restores the interrupt flag if the worker is interrupted.
     */
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

    /**
     * Returns the first terminal error observed by this worker.
     *
     * @return terminal error, or {@code null} when no unrecoverable failure has been captured
     */
    public Throwable getThrowable() {
        return throwable;
    }

    /**
     * Returns the table identifier currently being written.
     *
     * @return current table identifier, or {@code null} when no row has selected a writer yet
     */
    public String getCurrentTableId() {
        return currentTableId;
    }

    /**
     * Returns whether this worker is currently processing a dequeued row.
     *
     * @return {@code true} while a data row is in progress
     */
    public boolean isProcessingRow() {
        return processingRow;
    }

    /**
     * Returns whether this worker is notifying the parent about an isolated table failure.
     *
     * @return {@code true} while table-failure notification is in progress
     */
    public boolean isHandlingTableFailure() {
        return handlingTableFailure;
    }

    /**
     * Removes the writer for a failed table so subsequent rows for that table are skipped.
     *
     * @param tableId failed table identifier
     */
    public synchronized void removeTableWriter(String tableId) {
        tableIdWriterMap.remove(tableId);
    }

    /** Creates one ordered queue element that writes a data row. */
    static QueueElement rowRequest(SeaTunnelRow row) {
        return new RowWriteRequest(row);
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
    }

    private static class RowWriteRequest implements QueueElement {

        private final SeaTunnelRow row;

        private RowWriteRequest(SeaTunnelRow row) {
            this.row = row;
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

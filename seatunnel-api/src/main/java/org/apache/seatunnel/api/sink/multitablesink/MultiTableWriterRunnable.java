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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Async worker that drains one {@link BlockingQueue} and dispatches each {@link SeaTunnelRow} to
 * the {@link SinkWriter} registered for its table.
 *
 * <p>The parent {@link MultiTableSinkWriter} synchronizes on this runnable before lifecycle
 * operations that interact with sub-writers. Holding the same monitor during row writes keeps those
 * lifecycle operations from racing with an active write.
 *
 * <p>When table-level failure isolation is enabled, write failures are reported through the
 * configured handler and the failed table writer is removed so other tables can continue.
 */
@Slf4j
public class MultiTableWriterRunnable implements Runnable {

    private final Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap;
    private final BlockingQueue<SeaTunnelRow> queue;
    private final boolean allowSingleWriterFallback;
    private final boolean continueOnTableFailure;
    private final BiConsumer<String, Throwable> failureHandler;
    private final int tableRetryTimes;
    private final int tableRetryIntervalSeconds;
    private volatile Throwable throwable;
    private volatile String currentTableId;
    private volatile boolean processingRow;
    private volatile boolean handlingTableFailure;

    /**
     * Creates a worker that stops on the first table write failure.
     *
     * @param tableIdWriterMap writers keyed by table identifier
     * @param queue row queue owned by this runnable
     */
    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<SeaTunnelRow> queue) {
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
            BlockingQueue<SeaTunnelRow> queue,
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
            BlockingQueue<SeaTunnelRow> queue,
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
            SeaTunnelRow row = null;
            TableFailure tableFailure = null;
            try {
                row = queue.poll(100, TimeUnit.MILLISECONDS);
                if (row == null) {
                    continue;
                }
                processingRow = true;
                // control rows used for schema evolution / coordination
                // are represented as SeaTunnelRow with zero fields (arity == 0)
                if (row.getArity() == 0) {
                    log.debug(
                            "Skip control SeaTunnelRow with zero arity in MultiTableWriterRunnable: {}",
                            row);
                    processingRow = false;
                    continue;
                }
                synchronized (this) {
                    SinkWriter<SeaTunnelRow, ?, ?> writer = tableIdWriterMap.get(row.getTableId());
                    if (writer == null) {
                        // Single-table jobs may still emit rewritten/non-canonical table ids.
                        // Keep the historical sole-writer fallback only for runnables that
                        // started with one writer so quarantined multi-table rows are not rerouted.
                        if (allowSingleWriterFallback && tableIdWriterMap.size() == 1) {
                            writer = tableIdWriterMap.values().stream().findFirst().get();
                            currentTableId = tableIdWriterMap.keySet().stream().findFirst().get();
                        } else if (continueOnTableFailure) {
                            log.debug("Skip row for quarantined table {}", row.getTableId());
                            processingRow = false;
                            continue;
                        } else {
                            throw new RuntimeException(
                                    "MultiTableWriterRunnable can't find writer for tableId: "
                                            + row.getTableId());
                        }
                    } else {
                        currentTableId = row.getTableId();
                    }
                    try {
                        writeWithRetry(writer, row, currentTableId);
                        processingRow = false;
                    } catch (InterruptedException e) {
                        processingRow = false;
                        throw e;
                    } catch (Throwable e) {
                        tableFailure = handleWriteFailure(row, e);
                        if (tableFailure == null) {
                            processingRow = false;
                            break;
                        }
                    }
                }
                if (tableFailure != null) {
                    if (notifyTableFailure(tableFailure)) {
                        continue;
                    }
                    break;
                }
            } catch (InterruptedException e) {
                // When the job finished, the thread will be interrupted, so we ignore this
                // exception.
                processingRow = false;
                break;
            } catch (Throwable e) {
                tableFailure = handleWriteFailure(row, e);
                if (tableFailure != null && notifyTableFailure(tableFailure)) {
                    continue;
                }
                processingRow = false;
                break;
            }
        }
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

    private static class TableFailure {
        private final String tableId;
        private final Throwable error;

        private TableFailure(String tableId, Throwable error) {
            this.tableId = tableId;
            this.error = error;
        }
    }
}

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

@Slf4j
public class MultiTableWriterRunnable implements Runnable {

    private final Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap;
    private final BlockingQueue<SeaTunnelRow> queue;
    private final boolean allowSingleWriterFallback;
    private final boolean continueOnTableFailure;
    private final BiConsumer<String, Throwable> failureHandler;
    private volatile Throwable throwable;
    private volatile String currentTableId;

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<SeaTunnelRow> queue) {
        this(tableIdWriterMap, queue, false, (tableId, error) -> {});
    }

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<SeaTunnelRow> queue,
            boolean continueOnTableFailure,
            BiConsumer<String, Throwable> failureHandler) {
        this.tableIdWriterMap = tableIdWriterMap;
        this.queue = queue;
        this.allowSingleWriterFallback = tableIdWriterMap.size() == 1;
        this.continueOnTableFailure = continueOnTableFailure;
        this.failureHandler = failureHandler;
    }

    @Override
    public void run() {
        while (true) {
            SeaTunnelRow row = null;
            try {
                row = queue.poll(100, TimeUnit.MILLISECONDS);
                if (row == null) {
                    continue;
                }
                // control rows used for schema evolution / coordination
                // are represented as SeaTunnelRow with zero fields (arity == 0)
                if (row.getArity() == 0) {
                    log.debug(
                            "Skip control SeaTunnelRow with zero arity in MultiTableWriterRunnable: {}",
                            row);
                    continue;
                }
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
                        continue;
                    } else {
                        throw new RuntimeException(
                                "MultiTableWriterRunnable can't find writer for tableId: "
                                        + row.getTableId());
                    }
                } else {
                    currentTableId = row.getTableId();
                }
                synchronized (this) {
                    writer.write(row);
                }
            } catch (InterruptedException e) {
                // When the job finished, the thread will be interrupted, so we ignore this
                // exception.
                break;
            } catch (Throwable e) {
                log.error(
                        String.format("MultiTableWriterRunnable error when write row %s", row), e);
                String failedTableId =
                        currentTableId != null
                                ? currentTableId
                                : row == null ? null : row.getTableId();
                if (continueOnTableFailure
                        && failedTableId != null
                        && !failedTableId.trim().isEmpty()) {
                    removeTableWriter(failedTableId);
                    failureHandler.accept(failedTableId, e);
                    currentTableId = null;
                    continue;
                }
                throwable = e;
                break;
            }
        }
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getCurrentTableId() {
        return currentTableId;
    }

    public synchronized void removeTableWriter(String tableId) {
        tableIdWriterMap.remove(tableId);
    }
}

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

@Slf4j
public class MultiTableWriterRunnable implements Runnable {

    private final Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap;
    private final BlockingQueue<SeaTunnelRow> queue;
    private volatile Throwable throwable;
    private volatile String currentTableId;
    private volatile MultiTableRowErrorHandler rowErrorHandler;

    public MultiTableWriterRunnable(
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap,
            BlockingQueue<SeaTunnelRow> queue) {
        this.tableIdWriterMap = tableIdWriterMap;
        this.queue = queue;
    }

    public void setRowErrorHandler(MultiTableRowErrorHandler rowErrorHandler) {
        this.rowErrorHandler = rowErrorHandler;
    }

    @Override
    public void run() {
        while (true) {
            SeaTunnelRow row = null;
            SinkWriter<SeaTunnelRow, ?, ?> writer = null;
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
                writer = tableIdWriterMap.get(row.getTableId());
                if (writer == null) {
                    if (tableIdWriterMap.size() == 1) {
                        writer = tableIdWriterMap.values().stream().findFirst().get();
                        currentTableId = tableIdWriterMap.keySet().stream().findFirst().get();
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
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable e) {
                log.error(
                        String.format("MultiTableWriterRunnable error when write row %s", row), e);

                if (row != null && rowErrorHandler != null && writer != null) {
                    try {
                        boolean handled =
                                rowErrorHandler.handleRowError(writer, currentTableId, row, e);
                        if (handled) {
                            // Error handled, continue processing.
                            continue;
                        }
                    } catch (Throwable handlerEx) {
                        log.error(
                                String.format(
                                        "RowErrorHandler threw exception when handling row %s",
                                        row),
                                handlerEx);
                        handlerEx.addSuppressed(e);
                        throwable = handlerEx;
                        break;
                    }
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
}

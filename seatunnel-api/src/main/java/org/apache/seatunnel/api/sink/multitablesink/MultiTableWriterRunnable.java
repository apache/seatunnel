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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MultiTableWriterRunnable implements Runnable {

    private final int queueIndex;
    private final int tableCount;
    private final BlockingQueue<SeaTunnelRow> queue;
    private final MultiTableSinkWriter sinkWriter;
    private volatile Throwable throwable;

    public MultiTableWriterRunnable(
            int queueIndex,
            BlockingQueue<SeaTunnelRow> queue,
            MultiTableSinkWriter sinkWriter,
            int tableCount) {
        this.queueIndex = queueIndex;
        this.tableCount = tableCount;
        this.queue = queue;
        this.sinkWriter = sinkWriter;
    }

    @Override
    public void run() {
        while (true) {
            try {
                synchronized (this) {
                    SeaTunnelRow row = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (row == null) {
                        Thread.yield();
                        continue;
                    }
                    SinkWriter<SeaTunnelRow, ?, ?> writer =
                            sinkWriter.getWriter(row.getTableId(), queueIndex);
                    if (writer == null) {
                        if (tableCount == 1) {
                            writer = sinkWriter.getWriter(null, queueIndex);
                        } else {
                            throw new RuntimeException(
                                    "MultiTableWriterRunnable can't find writer for tableId: "
                                            + row.getTableId());
                        }
                    }
                    writer.write(row);
                }
            } catch (InterruptedException e) {
                // When the job finished, the thread will be interrupted, so we ignore this
                // exception.
                throwable = e;
                break;
            } catch (Throwable e) {
                log.error("MultiTableWriterRunnable error", e);
                throwable = e;
                break;
            }
        }
    }

    public Throwable getThrowable() {
        return throwable;
    }
}

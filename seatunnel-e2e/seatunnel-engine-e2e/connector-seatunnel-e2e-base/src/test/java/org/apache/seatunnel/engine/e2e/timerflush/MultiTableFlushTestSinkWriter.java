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

package org.apache.seatunnel.engine.e2e.timerflush;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MultiTableFlushTestSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    public static final ConcurrentMap<String, AtomicInteger> FLUSH_COUNTS =
            new ConcurrentHashMap<>();

    public static final ConcurrentMap<String, AtomicLong> WRITE_COUNTS = new ConcurrentHashMap<>();

    public static final ConcurrentMap<String, AtomicLong> FLUSHED_ROW_TOTALS =
            new ConcurrentHashMap<>();

    public static final CopyOnWriteArrayList<FlushSnapshot> FLUSH_SNAPSHOTS =
            new CopyOnWriteArrayList<>();

    private final ConcurrentMap<String, AtomicLong> buffer = new ConcurrentHashMap<>();

    public MultiTableFlushTestSinkWriter(SinkWriter.Context context) {
        context.registerFlushAction(this::flush);
    }

    @Override
    public void write(SeaTunnelRow element) {
        String tableId = element.getTableId();
        if (tableId != null) {
            buffer.computeIfAbsent(tableId, k -> new AtomicLong(0)).incrementAndGet();
            WRITE_COUNTS.computeIfAbsent(tableId, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    private void flush() {
        List<FlushSnapshot.TableCount> tableCounts = new ArrayList<>();
        for (ConcurrentMap.Entry<String, AtomicLong> entry : buffer.entrySet()) {
            long count = entry.getValue().getAndSet(0);
            if (count > 0) {
                tableCounts.add(new FlushSnapshot.TableCount(entry.getKey(), count));
                FLUSH_COUNTS
                        .computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0))
                        .incrementAndGet();
                FLUSHED_ROW_TOTALS
                        .computeIfAbsent(entry.getKey(), k -> new AtomicLong(0))
                        .addAndGet(count);
            }
        }
        if (!tableCounts.isEmpty()) {
            FLUSH_SNAPSHOTS.add(
                    new FlushSnapshot(
                            System.nanoTime(), Thread.currentThread().getName(), tableCounts));
        }
    }

    @Override
    public void close() {}

    public static void reset() {
        FLUSH_COUNTS.clear();
        WRITE_COUNTS.clear();
        FLUSHED_ROW_TOTALS.clear();
        FLUSH_SNAPSHOTS.clear();
    }

    public static class FlushSnapshot {
        public final long timestampNanos;
        public final String threadName;
        public final List<TableCount> tableCounts;

        FlushSnapshot(long timestampNanos, String threadName, List<TableCount> tableCounts) {
            this.timestampNanos = timestampNanos;
            this.threadName = threadName;
            this.tableCounts = tableCounts;
        }

        public static class TableCount {
            public final String tableId;
            public final long rowCount;

            TableCount(String tableId, long rowCount) {
                this.tableId = tableId;
                this.rowCount = rowCount;
            }
        }
    }
}

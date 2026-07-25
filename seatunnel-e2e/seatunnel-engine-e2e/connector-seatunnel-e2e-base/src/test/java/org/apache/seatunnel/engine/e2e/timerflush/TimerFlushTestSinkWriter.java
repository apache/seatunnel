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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TimerFlushTestSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    public static final ConcurrentLinkedQueue<SeaTunnelRow> FLUSHED_ROWS =
            new ConcurrentLinkedQueue<>();
    public static final AtomicInteger FLUSH_COUNT = new AtomicInteger(0);

    private final List<SeaTunnelRow> buffer = new ArrayList<>();

    public TimerFlushTestSinkWriter(SinkWriter.Context context) {
        context.registerFlushAction(this::flush);
    }

    @Override
    public void write(SeaTunnelRow element) {
        buffer.add(element);
    }

    private void flush() {
        List<SeaTunnelRow> snapshot = new ArrayList<>(buffer);
        buffer.clear();
        FLUSHED_ROWS.addAll(snapshot);
        FLUSH_COUNT.incrementAndGet();
    }

    @Override
    public void close() {}

    public static void reset() {
        FLUSHED_ROWS.clear();
        FLUSH_COUNT.set(0);
    }
}

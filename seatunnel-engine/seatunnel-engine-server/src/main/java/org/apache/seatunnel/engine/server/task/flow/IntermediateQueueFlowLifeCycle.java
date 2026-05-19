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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.group.queue.AbstractIntermediateQueue;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;

import java.io.IOException;

/** Bridges an intermediate queue between upstream producers and downstream collectors. */
public class IntermediateQueueFlowLifeCycle<T extends AbstractIntermediateQueue<?>>
        extends AbstractFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>>, OneOutputFlowLifeCycle<Record<?>> {

    private final AbstractIntermediateQueue<?> queue;

    private volatile Counter stainTraceEntriesTruncatedTotal;
    private volatile int stainTraceMaxEntriesPerTrace = -1;

    public IntermediateQueueFlowLifeCycle(
            SeaTunnelTask runningTask,
            CompletableFuture<Void> completableFuture,
            AbstractIntermediateQueue<?> queue) {
        super(runningTask, completableFuture);
        this.queue = queue;
        queue.setIntermediateQueueFlowLifeCycle(this);
        queue.setRunningTask(runningTask);
    }

    /**
     * Delegates incoming records to the concrete queue implementation so it can record trace state.
     */
    @Override
    public void received(Record<?> record) {
        queue.received(record);
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        queue.collect(collector);
    }

    @Override
    public void close() throws IOException {
        queue.close();
        super.close();
    }

    public Counter getStainTraceEntriesTruncatedTotal() {
        if (stainTraceEntriesTruncatedTotal == null) {
            synchronized (this) {
                if (stainTraceEntriesTruncatedTotal == null) {
                    stainTraceEntriesTruncatedTotal =
                            runningTask
                                    .getMetricsContext()
                                    .counter(StainTraceConstants.METRIC_ENTRIES_TRUNCATED_TOTAL);
                }
            }
        }
        return stainTraceEntriesTruncatedTotal;
    }

    public int getStainTraceMaxEntriesPerTrace() {
        if (stainTraceMaxEntriesPerTrace < 0) {
            synchronized (this) {
                if (stainTraceMaxEntriesPerTrace < 0) {
                    stainTraceMaxEntriesPerTrace =
                            runningTask
                                    .getExecutionContext()
                                    .getTaskExecutionService()
                                    .getSeaTunnelConfig()
                                    .getEngineConfig()
                                    .getStainTraceMaxEntriesPerTrace();
                }
            }
        }
        return stainTraceMaxEntriesPerTrace;
    }
}

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

package org.apache.seatunnel.engine.server.task.group.queue;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.signal.Signal;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.common.utils.function.ConsumerWithException;
import org.apache.seatunnel.common.utils.function.FunctionWithException;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.flush.FlushSignalConstants;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;
import org.apache.seatunnel.engine.server.trace.StainTraceUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** Blocking-queue implementation that records queue-stage stain trace entries on buffered rows. */
@Slf4j
public class IntermediateBlockingQueue extends AbstractIntermediateQueue<BlockingQueue<Record<?>>> {

    private final Counter totalIntermediateQueueSize;
    private final Counter intermediateQueueSize;
    private final Counter putBlockedNs;
    private volatile Counter stainTraceEntriesTruncatedTotal;

    private volatile Counter flushSignalTotal;

    private volatile Counter flushSignalSuccessTotal;

    private volatile Counter flushSignalFailureTotal;

    private volatile int stainTraceMaxEntriesPerTrace = -1;

    public IntermediateBlockingQueue(
            BlockingQueue<Record<?>> queue,
            Counter totalIntermediateQueueSize,
            Counter intermediateQueueSize,
            Counter putBlockedNs) {
        super(queue);
        this.totalIntermediateQueueSize = totalIntermediateQueueSize;
        this.intermediateQueueSize = intermediateQueueSize;
        this.putBlockedNs = putBlockedNs;
    }

    @Override
    public void received(Record<?> record) {
        boolean result;
        try {
            boolean metricsEnabled =
                    getRunningTask() != null && getRunningTask().isObservabilityEnabled();
            if (record.getData() instanceof Signal) {
                result = handleSignalRecord(record, getIntermediateQueue()::offer);
            } else {
                result =
                        handleRecord(
                                record,
                                r -> {
                                    if (!metricsEnabled) {
                                        getIntermediateQueue().put(r);
                                        return;
                                    }
                                    if (!getIntermediateQueue().offer(r)) {
                                        long blockedStartNs = System.nanoTime();
                                        getIntermediateQueue().put(r);
                                        putBlockedNs.inc(System.nanoTime() - blockedStartNs);
                                    }
                                },
                                StainTraceStage.QUEUE_IN);
            }
            if (result) {
                totalIntermediateQueueSize.inc();
                if (metricsEnabled) {
                    intermediateQueueSize.set(getIntermediateQueue().size());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        boolean metricsEnabled =
                getRunningTask() != null && getRunningTask().isObservabilityEnabled();
        while (true) {
            Record<?> record = getIntermediateQueue().poll(100, TimeUnit.MILLISECONDS);
            if (record == null) {
                break;
            }
            boolean result = handleRecord(record, collector::collect, StainTraceStage.QUEUE_OUT);
            if (result) {
                totalIntermediateQueueSize.dec();
                if (metricsEnabled) {
                    intermediateQueueSize.set(getIntermediateQueue().size());
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        getIntermediateQueue().clear();
    }

    private boolean handleRecord(
            Record<?> record, ConsumerWithException<Record<?>> consumer, StainTraceStage stage)
            throws Exception {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            getRunningTask().ack(barrier);
            if (barrier.prepareClose(this.getRunningTask().getTaskLocation())) {
                getIntermediateQueueFlowLifeCycle().setPrepareClose(true);
            }
            consumer.accept(record);
        } else if (record.getData() instanceof Signal) {
            if (getIntermediateQueueFlowLifeCycle().getPrepareClose()) {
                return false;
            }
            if (stage == StainTraceStage.QUEUE_OUT) {
                getFlushSignalSuccessTotal().inc();
            }
            consumer.accept(record);
        } else {
            if (getIntermediateQueueFlowLifeCycle().getPrepareClose()) {
                return false;
            }
            if (stage != null && record.getData() instanceof SeaTunnelRow) {
                SeaTunnelRow row = (SeaTunnelRow) record.getData();
                if (StainTraceUtils.hasPayload(row)) {
                    StainTraceUtils.appendIfPresent(
                            row,
                            stage,
                            getRunningTask().getTaskID(),
                            System.currentTimeMillis(),
                            getStainTraceMaxEntriesPerTrace(),
                            getStainTraceEntriesTruncatedTotal());
                }
            }
            consumer.accept(record);
        }

        return true;
    }

    private boolean handleSignalRecord(
            Record<?> record, FunctionWithException<Record<?>, Boolean, Exception> function)
            throws Exception {
        if (getIntermediateQueueFlowLifeCycle().getPrepareClose()) {
            return false;
        }
        boolean offered = function.apply(record);
        getFlushSignalTotal().inc();
        if (!offered) {
            getFlushSignalFailureTotal().inc();
        }

        return offered;
    }

    private Counter getStainTraceEntriesTruncatedTotal() {
        if (stainTraceEntriesTruncatedTotal == null) {
            synchronized (this) {
                if (stainTraceEntriesTruncatedTotal == null) {
                    stainTraceEntriesTruncatedTotal =
                            getRunningTask()
                                    .getMetricsContext()
                                    .counter(StainTraceConstants.METRIC_ENTRIES_TRUNCATED_TOTAL);
                }
            }
        }
        return stainTraceEntriesTruncatedTotal;
    }

    private int getStainTraceMaxEntriesPerTrace() {
        if (stainTraceMaxEntriesPerTrace < 0) {
            synchronized (this) {
                if (stainTraceMaxEntriesPerTrace < 0) {
                    stainTraceMaxEntriesPerTrace =
                            getRunningTask()
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

    public Counter getFlushSignalTotal() {
        if (flushSignalTotal == null) {
            synchronized (this) {
                if (flushSignalTotal == null) {
                    flushSignalTotal =
                            getRunningTask()
                                    .getMetricsContext()
                                    .counter(FlushSignalConstants.METRIC_FLUSH_SIGNAL_TOTAL);
                }
            }
        }
        return stainTraceEntriesTruncatedTotal;
    }

    public Counter getFlushSignalSuccessTotal() {
        if (flushSignalSuccessTotal == null) {
            synchronized (this) {
                if (flushSignalSuccessTotal == null) {
                    flushSignalSuccessTotal =
                            getRunningTask()
                                    .getMetricsContext()
                                    .counter(
                                            FlushSignalConstants.METRIC_FLUSH_SIGNAL_SUCCESS_TOTAL);
                }
            }
        }
        return flushSignalSuccessTotal;
    }

    public Counter getFlushSignalFailureTotal() {
        if (flushSignalFailureTotal == null) {
            synchronized (this) {
                if (flushSignalFailureTotal == null) {
                    flushSignalFailureTotal =
                            getRunningTask()
                                    .getMetricsContext()
                                    .counter(
                                            FlushSignalConstants.METRIC_FLUSH_SIGNAL_FAILURE_TOTAL);
                }
            }
        }
        return flushSignalFailureTotal;
    }
}

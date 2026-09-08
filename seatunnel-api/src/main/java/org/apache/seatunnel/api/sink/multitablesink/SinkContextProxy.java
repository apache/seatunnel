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

import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.common.utils.function.RunnableWithException;

import java.util.Objects;
import java.util.Optional;

public class SinkContextProxy implements SinkWriter.Context {

    private final int index;
    private final int replicaNum;
    private final SinkWriter.Context context;
    private transient volatile RunnableWithException flushAction;

    public SinkContextProxy(int index, int replicaNum, SinkWriter.Context context) {
        this.index = index;
        this.replicaNum = replicaNum;
        this.context = context;
    }

    @Override
    public int getIndexOfSubtask() {
        return index;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return context.getNumberOfParallelSubtasks() * replicaNum;
    }

    @Override
    public MetricsContext getMetricsContext() {
        return context.getMetricsContext();
    }

    @Override
    public EventListener getEventListener() {
        return context.getEventListener();
    }

    @Override
    public Optional<RowErrorCollector> getRowErrorCollector() {
        return context.getRowErrorCollector();
    }

    @Override
    public void enableDeferredTerminalWriteOutcomes() {
        context.enableDeferredTerminalWriteOutcomes();
    }

    @Override
    public boolean isDeferredTerminalWriteOutcomesEnabled() {
        return context.isDeferredTerminalWriteOutcomesEnabled();
    }

    @Override
    public void registerFlushAction(RunnableWithException action) {
        Objects.requireNonNull(action, "flushAction");
        this.flushAction = action;
    }

    @Override
    public RunnableWithException getFlushAction() {
        return flushAction;
    }
}

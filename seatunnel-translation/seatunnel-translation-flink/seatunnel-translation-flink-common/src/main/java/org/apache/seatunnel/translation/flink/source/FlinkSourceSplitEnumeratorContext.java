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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.List;
import java.util.Set;

/**
 * The implementation of {@link org.apache.seatunnel.api.source.SourceSplitEnumerator.Context} for
 * flink engine.
 *
 * @param <SplitT>
 */
public class FlinkSourceSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    private final SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext;
    protected final EventListener eventListener;

    public FlinkSourceSplitEnumeratorContext(
            SplitEnumeratorContext<SplitWrapper<SplitT>> enumContext) {
        this.enumContext = enumContext;
        this.eventListener = new DefaultEventProcessor();
    }

    @Override
    public int currentParallelism() {
        return enumContext.currentParallelism();
    }

    @Override
    public Set<Integer> registeredReaders() {
        return enumContext.registeredReaders().keySet();
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        splits.forEach(
                split -> {
                    enumContext.assignSplit(new SplitWrapper<>(split), subtaskId);
                });
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        enumContext.signalNoMoreSplits(subtask);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        enumContext.sendEventToSourceReader(subtaskId, new SourceEventWrapper(event));
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new AbstractMetricsContext() {};
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }
}

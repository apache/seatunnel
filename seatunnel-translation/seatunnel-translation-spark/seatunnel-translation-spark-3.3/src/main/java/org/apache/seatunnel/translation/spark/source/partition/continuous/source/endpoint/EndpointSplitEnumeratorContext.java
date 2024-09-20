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

package org.apache.seatunnel.translation.spark.source.partition.continuous.source.endpoint;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

public class EndpointSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {
    protected final int parallelism;
    protected final EventListener eventListener;
    protected volatile boolean running = false;
    private final LinkedBlockingDeque<SplitT>[] queues;

    public EndpointSplitEnumeratorContext(int parallelism, String jobId) {
        this.parallelism = parallelism;
        this.queues = new LinkedBlockingDeque[this.parallelism];
        this.eventListener = new DefaultEventProcessor(jobId);
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Set<Integer> registeredReaders() {
        Set<Integer> readers = new HashSet<>();
        if (running) {
            for (int i = 0; i < queues.length; i++) {
                if (queues[i] != null) {
                    readers.add(i);
                }
            }
        }
        return readers;
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        if (subtaskId < 0 || subtaskId >= parallelism) {
            return;
        }

        LinkedBlockingDeque<SplitT> queue = queues[subtaskId];
        if (queue == null) {
            queue = new LinkedBlockingDeque<>();
        }

        queue.addAll(splits);
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        queues[subtask] = null;
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        throw new UnsupportedOperationException("Unsupported");
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

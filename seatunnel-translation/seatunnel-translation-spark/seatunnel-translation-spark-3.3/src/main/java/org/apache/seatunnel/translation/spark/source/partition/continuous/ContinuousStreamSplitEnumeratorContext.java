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

package org.apache.seatunnel.translation.spark.source.partition.continuous;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ContinuousStreamSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {
    private final int parallelism;
    private final EventListener eventListener;
    private final Set<Integer> readers = new HashSet<>();

    public ContinuousStreamSplitEnumeratorContext(int parallelism, EventListener eventListener) {
        this.parallelism = parallelism;
        this.eventListener = eventListener;
    }

    @Override
    public int currentParallelism() {
        return this.parallelism;
    }

    @Override
    public Set<Integer> registeredReaders() {
        return this.readers;
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {}

    @Override
    public void signalNoMoreSplits(int subtask) {}

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        throw new UnsupportedOperationException(
                "Flink ParallelSource don't support sending SourceEvent. "
                        + "Please implement the `SupportCoordinate` marker interface on the SeaTunnel source.");
    }

    @Override
    public MetricsContext getMetricsContext() {
        // TODO Waiting for Flink and Spark to implement MetricsContext
        // https://github.com/apache/seatunnel/issues/3431
        return new AbstractMetricsContext() {};
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }
}

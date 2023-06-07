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

package org.apache.seatunnel.engine.server.task.context;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.operation.source.AssignSplitOperation;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class SeaTunnelSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    private final int parallelism;

    private final SourceSplitEnumeratorTask<SplitT> task;

    private final MetricsContext metricsContext;

    public SeaTunnelSplitEnumeratorContext(
            int parallelism,
            SourceSplitEnumeratorTask<SplitT> task,
            MetricsContext metricsContext) {
        this.parallelism = parallelism;
        this.task = task;
        this.metricsContext = metricsContext;
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Set<Integer> registeredReaders() {
        return new HashSet<>(task.getRegisteredReaders());
    }

    @Override
    public void assignSplit(int subtaskIndex, List<SplitT> splits) {
        if (registeredReaders().isEmpty()) {
            log.warn("No reader is obtained, skip this assign!");
            return;
        }
        task.getExecutionContext()
                .sendToMember(
                        new AssignSplitOperation<>(
                                task.getTaskMemberLocationByIndex(subtaskIndex),
                                SerializationUtils.serialize(splits.toArray())),
                        task.getTaskMemberAddressByIndex(subtaskIndex))
                .join();
    }

    @Override
    public void signalNoMoreSplits(int subtaskIndex) {
        task.getExecutionContext()
                .sendToMember(
                        new AssignSplitOperation<>(
                                task.getTaskMemberLocationByIndex(subtaskIndex),
                                SerializationUtils.serialize(Collections.emptyList().toArray())),
                        task.getTaskMemberAddressByIndex(subtaskIndex))
                .join();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

    @Override
    public MetricsContext getMetricsContext() {
        return metricsContext;
    }
}

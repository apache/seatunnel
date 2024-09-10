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

package org.apache.seatunnel.translation.spark.source.partition.continuous.source.rpc;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.spark.rpc.RpcEndpointRef;

import java.util.List;
import java.util.Set;

public class RpcSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {
    private final RpcEndpointRef driverRef;

    public RpcSplitEnumeratorContext(RpcEndpointRef driverRef) {
        this.driverRef = driverRef;
    }

    @Override
    public int currentParallelism() {
        return 0;
    }

    @Override
    public Set<Integer> registeredReaders() {
        return null;
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {}

    @Override
    public void signalNoMoreSplits(int subtask) {}

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

    @Override
    public MetricsContext getMetricsContext() {
        return null;
    }

    @Override
    public EventListener getEventListener() {
        return null;
    }
}

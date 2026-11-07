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

package org.apache.seatunnel.engine.server.autoscale;

import org.apache.seatunnel.engine.common.config.server.AutoscalerConfig;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceShortageListener;

import com.hazelcast.cluster.Address;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Stores worker resource samples and capacity-shortage statistics for autoscaler evaluation.
 *
 * <p>Worker metrics reports update the sample store. As a {@link ResourceShortageListener}, this
 * class records capacity shortages detected during slot allocation. The autoscaler reads summaries
 * of both. The coordinator closes this store on master demotion so late reports cannot affect the
 * next master term.
 */
public final class AutoscalerSignalStore implements ResourceShortageListener {
    private final boolean enabled;
    private final WorkerResourceSampleStore workerSamples;
    private final ResourceShortageStats shortages = new ResourceShortageStats();
    private volatile boolean closed;

    public AutoscalerSignalStore(AutoscalerConfig config) {
        this.enabled = config.isEnabled();
        this.workerSamples =
                new WorkerResourceSampleStore(
                        TimeUnit.SECONDS.toMillis(config.getFutureTimestampToleranceSeconds()),
                        TimeUnit.SECONDS.toMillis(config.getMaxMetricStalenessSeconds()));
    }

    /** Accepts a worker sample only while this master incarnation is active. */
    public void record(WorkerMetricsSample sample, long receiveTimeMillis) {
        if (enabled && !closed) {
            workerSamples.record(sample, receiveTimeMillis);
        }
    }

    /** Records capacity pressure reported by the resource request path. */
    @Override
    public void recordShortage(
            boolean waitStrategy, int requestedResources, String resourceDescription) {
        if (!enabled || closed) {
            return;
        }
        if (waitStrategy) {
            shortages.recordWaitShortage(requestedResources, resourceDescription);
        } else {
            shortages.recordRejectShortage(requestedResources, resourceDescription);
        }
    }

    public void removeWorker(Address address) {
        workerSamples.remove(address);
    }

    public WorkerSampleSummary summarizeWorkers(Set<Address> workers, long nowMillis) {
        workerSamples.retainWorkers(workers);
        return workerSamples.summarize(workers, nowMillis);
    }

    public ResourceShortageSnapshot shortageSnapshot(long afterSequence) {
        return shortages.getIncrementalSnapshot(afterSequence);
    }

    /** Fences late reports from a demoted master's asynchronous resource requests. */
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}

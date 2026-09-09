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

import com.hazelcast.cluster.Address;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Keeps the latest accepted autoscaler metrics sample for each worker.
 *
 * <p>Samples with invalid utilization, timestamps beyond the allowed future tolerance, or
 * non-increasing event time are rejected before they influence a snapshot.
 */
public final class LatestWorkerSampleStore {

    private final long futureTimestampToleranceMillis;
    private final Map<Address, WorkerMetricsSample> samples = new HashMap<>();

    public LatestWorkerSampleStore(long futureTimestampToleranceMillis) {
        this.futureTimestampToleranceMillis = futureTimestampToleranceMillis;
    }

    /** Records a valid, newer sample for a worker. */
    public synchronized boolean record(WorkerMetricsSample sample, long nowMillis) {
        if (!isValidUtilization(sample.getCpuUtilization())
                || !isValidUtilization(sample.getJvmMemoryUtilization())) {
            return false;
        }
        // Reject samples that are too far ahead of the receiver clock.
        if (sample.getEventTimeMillis() - nowMillis > futureTimestampToleranceMillis) {
            return false;
        }
        WorkerMetricsSample previous = samples.get(sample.getWorkerAddress());
        if (previous != null && sample.getEventTimeMillis() <= previous.getEventTimeMillis()) {
            return false;
        }
        samples.put(sample.getWorkerAddress(), sample);
        return true;
    }

    public synchronized Optional<WorkerMetricsSample> getLatest(Address workerAddress) {
        return Optional.ofNullable(samples.get(workerAddress));
    }

    public synchronized void retainWorkers(Set<Address> currentWorkers) {
        Iterator<Address> iterator = samples.keySet().iterator();
        while (iterator.hasNext()) {
            if (!currentWorkers.contains(iterator.next())) {
                iterator.remove();
            }
        }
    }

    public synchronized void remove(Address workerAddress) {
        samples.remove(workerAddress);
    }

    public synchronized WorkerSampleSummary summarize(
            Set<Address> currentWorkers, long nowMillis, long freshnessMillis) {
        int valid = 0;
        int missing = 0;
        int stale = 0;
        int future = 0;
        double cpuSum = 0.0d;
        double jvmMemorySum = 0.0d;

        for (Address currentWorker : currentWorkers) {
            WorkerMetricsSample sample = samples.get(currentWorker);
            if (sample == null) {
                missing++;
                continue;
            }
            // Exclude samples that are too far ahead of the receiver clock.
            if (sample.getEventTimeMillis() > nowMillis + futureTimestampToleranceMillis) {
                future++;
                continue;
            }
            // Exclude samples that are older than the freshness window.
            if (nowMillis - sample.getEventTimeMillis() > freshnessMillis) {
                stale++;
                continue;
            }
            valid++;
            cpuSum += sample.getCpuUtilization();
            jvmMemorySum += sample.getJvmMemoryUtilization();
        }

        MetricValue cpu = valid == 0 ? MetricValue.missing() : MetricValue.valid(cpuSum / valid);
        MetricValue jvmMemory =
                valid == 0 ? MetricValue.missing() : MetricValue.valid(jvmMemorySum / valid);
        boolean scaleInMetricsValid =
                !currentWorkers.isEmpty()
                        && valid == currentWorkers.size()
                        && missing == 0
                        && stale == 0
                        && future == 0;

        return new WorkerSampleSummary(
                currentWorkers.size(),
                valid,
                missing,
                stale,
                future,
                cpu,
                jvmMemory,
                scaleInMetricsValid);
    }

    private static boolean isValidUtilization(double value) {
        return Double.isFinite(value) && value >= 0.0d && value <= 1.0d;
    }
}

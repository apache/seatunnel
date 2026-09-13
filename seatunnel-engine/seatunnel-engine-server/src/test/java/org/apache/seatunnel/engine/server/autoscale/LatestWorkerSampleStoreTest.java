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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

class LatestWorkerSampleStoreTest {

    private static final Address WORKER = address(5801);

    @Test
    void acceptsLatestValidSample() {
        LatestWorkerSampleStore store = new LatestWorkerSampleStore(5_000L);
        WorkerMetricsSample sample = new WorkerMetricsSample(WORKER, 1_000L, 0.5d, 0.6d);

        Assertions.assertTrue(store.record(sample, 1_000L));
        Optional<WorkerMetricsSample> latest = store.getLatest(WORKER);

        Assertions.assertTrue(latest.isPresent());
        Assertions.assertEquals(0.5d, latest.get().getCpuUtilization());
        Assertions.assertEquals(0.6d, latest.get().getJvmMemoryUtilization());
    }

    @Test
    void rejectsInvalidSamplesAndKeepsPreviousAcceptedValue() {
        LatestWorkerSampleStore store = new LatestWorkerSampleStore(5_000L);
        WorkerMetricsSample sample = new WorkerMetricsSample(WORKER, 1_000L, 0.5d, 0.6d);

        Assertions.assertTrue(store.record(sample, 1_000L));
        Assertions.assertFalse(
                store.record(new WorkerMetricsSample(WORKER, 2_000L, Double.NaN, 0.1d), 2_000L));
        Assertions.assertFalse(
                store.record(new WorkerMetricsSample(WORKER, 2_000L, 1.1d, 0.1d), 2_000L));
        Assertions.assertFalse(
                store.record(new WorkerMetricsSample(WORKER, 2_000L, 0.1d, -0.1d), 2_000L));

        Assertions.assertEquals(sample, store.getLatest(WORKER).get());
    }

    @Test
    void rejectsFutureAndOutOfOrderSamples() {
        LatestWorkerSampleStore store = new LatestWorkerSampleStore(5_000L);

        Assertions.assertFalse(
                store.record(new WorkerMetricsSample(WORKER, 7_001L, 0.1d, 0.1d), 1_000L));
        Assertions.assertTrue(
                store.record(new WorkerMetricsSample(WORKER, 2_000L, 0.2d, 0.2d), 2_000L));
        Assertions.assertFalse(
                store.record(new WorkerMetricsSample(WORKER, 1_999L, 0.3d, 0.3d), 3_000L));

        Assertions.assertEquals(2_000L, store.getLatest(WORKER).get().getEventTimeMillis());
    }

    @Test
    void removesSamplesForUnregisteredWorkers() {
        LatestWorkerSampleStore store = new LatestWorkerSampleStore(5_000L);
        Address other = address(5802);

        store.record(new WorkerMetricsSample(WORKER, 1_000L, 0.5d, 0.6d), 1_000L);
        store.record(new WorkerMetricsSample(other, 1_000L, 0.2d, 0.3d), 1_000L);

        store.retainWorkers(Collections.singleton(WORKER));

        Assertions.assertTrue(store.getLatest(WORKER).isPresent());
        Assertions.assertFalse(store.getLatest(other).isPresent());
    }

    @Test
    void classifiesSampleFreshnessForRegisteredWorkers() {
        LatestWorkerSampleStore store = new LatestWorkerSampleStore(5_000L);
        Address stale = address(5802);
        Address missing = address(5803);
        store.record(new WorkerMetricsSample(WORKER, 10_000L, 0.5d, 0.6d), 10_000L);
        store.record(new WorkerMetricsSample(stale, 1_000L, 0.2d, 0.3d), 1_000L);

        Set<Address> workers = new HashSet<>();
        workers.add(WORKER);
        workers.add(stale);
        workers.add(missing);

        WorkerSampleSummary summary = store.summarize(workers, 10_500L, 5_000L);

        Assertions.assertEquals(3, summary.getTotalSamples());
        Assertions.assertEquals(1, summary.getValidSamples());
        Assertions.assertEquals(1, summary.getStaleSamples());
        Assertions.assertEquals(1, summary.getMissingSamples());
        Assertions.assertEquals(MetricValue.valid(0.5d).getStatus(), summary.getCpu().getStatus());
        Assertions.assertFalse(summary.isScaleInMetricsValid());
    }

    private static Address address(int port) {
        try {
            return new Address("127.0.0.1", port);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }
}

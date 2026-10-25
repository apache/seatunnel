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

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.operation.ReportMetricsOperation;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_SIZE;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_IDLE_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READ_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_PROCESS_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.TRANSFORM_RECORDS_OUT;

/** Periodic worker metrics reports applied through the production operation and IMap path. */
@State(Scope.Thread)
public class IMapMetricsReportBenchmarkWorkload {

    private static final long METRICS_JOB_ID = Long.MAX_VALUE - 1_000_000L;

    @Param({"10", "100", "1000"})
    public int taskCount;

    private NodeEngineImpl nodeEngine;
    private MetricsSnapshotStateStore metricsSnapshotStore;
    private Map<TaskLocation, SeaTunnelMetricsContext> reportingSnapshot;

    /** Builds reports outside measured code and primes the existing metrics partition value. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) {
        nodeEngine = environment.getServer().getNodeEngine();
        metricsSnapshotStore = environment.getStateStores().metricsSnapshotStore();
        metricsSnapshotStore.merge(createSnapshot(1L));
        reportingSnapshot = createSnapshot(2L);
    }

    /** Executes the same master operation submitted by TaskExecutionService and awaits storage. */
    public void reportMetrics() throws Exception {
        nodeEngine
                .getOperationService()
                .createInvocationBuilder(
                        SeaTunnelServer.SERVICE_NAME,
                        new ReportMetricsOperation(reportingSnapshot),
                        nodeEngine.getMasterAddress())
                .invoke()
                .get();
    }

    /** Verifies off the clock that the operation replaced every previous task snapshot. */
    @TearDown(Level.Invocation)
    public void verifyReport() {
        boolean allSnapshotsStored =
                reportingSnapshot.entrySet().stream()
                        .allMatch(
                                entry -> {
                                    SeaTunnelMetricsContext stored =
                                            metricsSnapshotStore.get(entry.getKey());
                                    return stored != null
                                            && stored.counter(SOURCE_READ_NANOS).getCount()
                                                    == entry.getValue()
                                                            .counter(SOURCE_READ_NANOS)
                                                            .getCount();
                                });
        if (!allSnapshotsStored) {
            throw new IllegalStateException(
                    "The metrics report did not persist every task snapshot");
        }
    }

    private Map<TaskLocation, SeaTunnelMetricsContext> createSnapshot(long multiplier) {
        Map<TaskLocation, SeaTunnelMetricsContext> snapshot = new HashMap<>(taskCount);
        for (int index = 0; index < taskCount; index++) {
            TaskGroupLocation groupLocation = new TaskGroupLocation(METRICS_JOB_ID, 1, index + 1L);
            TaskLocation taskLocation = new TaskLocation(groupLocation, 0L, index);
            long value = multiplier * (index + 1L);

            SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
            metrics.counter(SOURCE_READ_NANOS).set(value * 100L);
            metrics.counter(SOURCE_IDLE_NANOS).set(value * 10L);
            metrics.counter(TRANSFORM_PROCESS_NANOS + "#1").set(value * 80L);
            metrics.counter(TRANSFORM_RECORDS_IN + "#1").set(value);
            metrics.counter(TRANSFORM_RECORDS_OUT + "#1").set(value);
            metrics.counter(SINK_WRITE_NANOS + "#2").set(value * 120L);
            metrics.counter(SINK_RECORDS_IN + "#2").set(value);
            metrics.counter(INTERMEDIATE_QUEUE_SIZE).set(index % 1024);
            snapshot.put(taskLocation, metrics);
        }
        return snapshot;
    }
}

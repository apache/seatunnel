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

package org.apache.seatunnel.engine.server.telemetry.metrics.exports;

import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.RequestSlotOperationStats;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

public class RequestSlotOperationExports extends AbstractCollector {

    public RequestSlotOperationExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();
        if (!isMaster() || !isCoordinatorReady()) {
            return mfs;
        }

        CoordinatorService coordinatorService = getReadyCoordinatorService();
        if (coordinatorService == null) {
            return mfs;
        }

        ResourceManager resourceManager = coordinatorService.getInitializedResourceManager();
        if (resourceManager == null) {
            return mfs;
        }

        String address = localAddress();
        RequestSlotOperationStats stats = resourceManager.getRequestSlotOperationStats();

        CounterMetricFamily totalMetricFamily =
                new CounterMetricFamily(
                        "request_slot_operation",
                        "The total number of RequestSlotOperation invocations sent by the master",
                        clusterLabelNames(ADDRESS, "result"));
        totalMetricFamily.addMetric(labelValues(address, "success"), stats.getSuccessCount());
        totalMetricFamily.addMetric(labelValues(address, "no_slot"), stats.getNoSlotCount());
        totalMetricFamily.addMetric(labelValues(address, "failure"), stats.getFailureCount());
        mfs.add(totalMetricFamily);

        GaugeMetricFamily lastLatencyMetricFamily =
                new GaugeMetricFamily(
                        "request_slot_operation_last_invocation_latency_ms",
                        "The most recent master-side RequestSlotOperation invocation latency "
                                + "in milliseconds, including master-to-worker invocation",
                        clusterLabelNames(ADDRESS));
        lastLatencyMetricFamily.addMetric(labelValues(address), stats.getLastInvocationLatencyMs());
        mfs.add(lastLatencyMetricFamily);

        GaugeMetricFamily maxLatencyMetricFamily =
                new GaugeMetricFamily(
                        "request_slot_operation_max_invocation_latency_ms",
                        "The maximum observed master-side RequestSlotOperation invocation "
                                + "latency in milliseconds since the master started, including "
                                + "master-to-worker invocation",
                        clusterLabelNames(ADDRESS));
        maxLatencyMetricFamily.addMetric(labelValues(address), stats.getMaxInvocationLatencyMs());
        mfs.add(maxLatencyMetricFamily);
        return mfs;
    }
}

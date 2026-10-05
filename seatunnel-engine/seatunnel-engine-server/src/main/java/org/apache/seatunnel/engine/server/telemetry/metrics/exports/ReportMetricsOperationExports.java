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

import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ReportMetricsOperationStats;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

public class ReportMetricsOperationExports extends AbstractCollector {

    public ReportMetricsOperationExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();
        TaskExecutionService taskExecutionService = getServer().getTaskExecutionService();
        if (taskExecutionService == null) {
            return mfs;
        }

        String address = localAddress();
        ReportMetricsOperationStats stats = taskExecutionService.getReportMetricsOperationStats();

        CounterMetricFamily totalMetricFamily =
                new CounterMetricFamily(
                        "report_metrics_operation",
                        "The total number of ReportMetricsOperation invocations sent by a worker",
                        clusterLabelNames(ADDRESS, "result"));
        totalMetricFamily.addMetric(labelValues(address, "success"), stats.getSuccessCount());
        totalMetricFamily.addMetric(labelValues(address, "failure"), stats.getFailureCount());
        totalMetricFamily.addMetric(
                labelValues(address, "interrupted"), stats.getInterruptedCount());
        mfs.add(totalMetricFamily);

        GaugeMetricFamily payloadMetricFamily =
                new GaugeMetricFamily(
                        "report_metrics_operation_last_payload_task_count",
                        "The number of task metrics included in the most recent "
                                + "ReportMetricsOperation payload sent by a worker",
                        clusterLabelNames(ADDRESS));
        payloadMetricFamily.addMetric(labelValues(address), stats.getLastPayloadTaskCount());
        mfs.add(payloadMetricFamily);

        GaugeMetricFamily lastLatencyMetricFamily =
                new GaugeMetricFamily(
                        "report_metrics_operation_last_invocation_latency_ms",
                        "The most recent worker-side ReportMetricsOperation reporting latency "
                                + "in milliseconds, including local metrics collection and "
                                + "worker-to-master invocation",
                        clusterLabelNames(ADDRESS));
        lastLatencyMetricFamily.addMetric(labelValues(address), stats.getLastInvocationLatencyMs());
        mfs.add(lastLatencyMetricFamily);

        GaugeMetricFamily maxLatencyMetricFamily =
                new GaugeMetricFamily(
                        "report_metrics_operation_max_invocation_latency_ms",
                        "The maximum observed worker-side ReportMetricsOperation reporting "
                                + "latency in milliseconds since the worker started, "
                                + "including local metrics collection and worker-to-master "
                                + "invocation",
                        clusterLabelNames(ADDRESS));
        maxLatencyMetricFamily.addMetric(labelValues(address), stats.getMaxInvocationLatencyMs());
        mfs.add(maxLatencyMetricFamily);
        return mfs;
    }
}

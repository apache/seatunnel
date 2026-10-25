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

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.service.jar.ConnectorPackageService;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EngineStateStoreLogicalMetricExports extends AbstractCollector {

    private static final String BACKEND = "hazelcast";

    public EngineStateStoreLogicalMetricExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<String> commonLabelNames = clusterLabelNames("backend");
        List<String> storeLabelNames = clusterLabelNames("store", "backend");

        GaugeMetricFamily runningJobMetricsTaskContexts =
                new GaugeMetricFamily(
                        "engine_state_store_running_job_metrics_task_contexts",
                        "Logical task metrics contexts stored in engine_runningJobMetrics",
                        commonLabelNames);
        GaugeMetricFamily runningJobMetricsActivePartitionKeys =
                new GaugeMetricFamily(
                        "engine_state_store_running_job_metrics_active_partition_keys",
                        "Active partition-key count stored in engine_runningJobMetrics",
                        commonLabelNames);
        GaugeMetricFamily checkpointMonitorJobs =
                new GaugeMetricFamily(
                        "engine_state_store_checkpoint_monitor_jobs",
                        "Job count stored in engine_checkpoint_monitor",
                        commonLabelNames);
        GaugeMetricFamily checkpointMonitorInProgressCheckpoints =
                new GaugeMetricFamily(
                        "engine_state_store_checkpoint_monitor_in_progress_checkpoints",
                        "In-progress checkpoint count stored in engine_checkpoint_monitor",
                        commonLabelNames);
        GaugeMetricFamily checkpointMonitorRetainedHistoryEntries =
                new GaugeMetricFamily(
                        "engine_state_store_checkpoint_monitor_retained_history_entries",
                        "Retained checkpoint history entry count stored in engine_checkpoint_monitor",
                        commonLabelNames);
        GaugeMetricFamily finishedJobRecords =
                new GaugeMetricFamily(
                        "engine_state_store_finished_job_records",
                        "Finished job records stored in finished job state stores",
                        storeLabelNames);
        CounterMetricFamily finishedJobCleanupTotal =
                new CounterMetricFamily(
                        "engine_state_store_finished_job_cleanup_total",
                        "Cleanup total for finished job state stores",
                        storeLabelNames);
        GaugeMetricFamily connectorJarTrackedJars =
                new GaugeMetricFamily(
                        "engine_state_store_connector_jar_tracked_jars",
                        "Tracked connector jar count stored in engine_connectorJarRefCounters",
                        commonLabelNames);
        GaugeMetricFamily connectorJarTotalReferences =
                new GaugeMetricFamily(
                        "engine_state_store_connector_jar_total_references",
                        "Total connector jar references stored in engine_connectorJarRefCounters",
                        commonLabelNames);

        if (!isMaster() || !isCoordinatorReady()) {
            return metricFamilies(
                    runningJobMetricsTaskContexts,
                    runningJobMetricsActivePartitionKeys,
                    checkpointMonitorJobs,
                    checkpointMonitorInProgressCheckpoints,
                    checkpointMonitorRetainedHistoryEntries,
                    finishedJobRecords,
                    finishedJobCleanupTotal,
                    connectorJarTrackedJars,
                    connectorJarTotalReferences);
        }

        runSafely(
                "engine_state_store_running_job_metrics",
                () -> {
                    addMetric(
                            runningJobMetricsTaskContexts,
                            getCoordinatorService().getRunningJobMetricsTaskContextCount(),
                            labelValues(BACKEND));
                    addMetric(
                            runningJobMetricsActivePartitionKeys,
                            getCoordinatorService().getRunningJobMetricsPartitionKeyCount(),
                            labelValues(BACKEND));
                });

        SeaTunnelServer server = getServer();
        runSafely(
                "engine_state_store_checkpoint_monitor",
                () -> {
                    addMetric(
                            checkpointMonitorJobs,
                            server.getCheckpointMonitorService().getOverviewJobCount(),
                            labelValues(BACKEND));
                    addMetric(
                            checkpointMonitorInProgressCheckpoints,
                            server.getCheckpointMonitorService().getInProgressCheckpointCount(),
                            labelValues(BACKEND));
                    addMetric(
                            checkpointMonitorRetainedHistoryEntries,
                            server.getCheckpointMonitorService().getRetainedHistoryCount(),
                            labelValues(BACKEND));
                });

        runSafely(
                "engine_state_store_finished_job",
                () -> {
                    JobHistoryService jobHistoryService =
                            getCoordinatorService().getJobHistoryService();
                    addStoreMetrics(
                            finishedJobRecords, jobHistoryService.getFinishedJobRecordCounts());
                    addStoreMetrics(
                            finishedJobCleanupTotal,
                            jobHistoryService.getFinishedJobCleanupTotals());
                });

        runSafely(
                "engine_state_store_connector_jar",
                () -> {
                    ConnectorPackageService connectorPackageService = null;
                    try {
                        connectorPackageService = server.getConnectorPackageService();
                    } catch (Exception e) {
                        getLogger(getClass())
                                .fine(
                                        "Connector package service is not enabled; exporting fallback metrics");
                    }
                    addMetric(
                            connectorJarTrackedJars,
                            getTrackedConnectorJarCount(connectorPackageService),
                            labelValues(BACKEND));
                    addMetric(
                            connectorJarTotalReferences,
                            getTotalConnectorJarReferences(connectorPackageService),
                            labelValues(BACKEND));
                });

        return metricFamilies(
                runningJobMetricsTaskContexts,
                runningJobMetricsActivePartitionKeys,
                checkpointMonitorJobs,
                checkpointMonitorInProgressCheckpoints,
                checkpointMonitorRetainedHistoryEntries,
                finishedJobRecords,
                finishedJobCleanupTotal,
                connectorJarTrackedJars,
                connectorJarTotalReferences);
    }

    private List<MetricFamilySamples> metricFamilies(MetricFamilySamples... samples) {
        List<MetricFamilySamples> metrics = new ArrayList<>();
        for (MetricFamilySamples sample : samples) {
            metrics.add(sample);
        }
        return metrics;
    }

    private void addStoreMetrics(GaugeMetricFamily metricFamily, Map<String, Long> storeMetrics) {
        storeMetrics.forEach(
                (storeName, value) ->
                        addMetric(metricFamily, value, labelValues(storeName, BACKEND)));
    }

    private void addStoreMetrics(CounterMetricFamily metricFamily, Map<String, Long> storeMetrics) {
        storeMetrics.forEach(
                (storeName, value) ->
                        addMetric(metricFamily, value, labelValues(storeName, BACKEND)));
    }

    private void addMetric(GaugeMetricFamily metricFamily, long value, List<String> labels) {
        metricFamily.addMetric(labels, value);
    }

    private void addMetric(CounterMetricFamily metricFamily, long value, List<String> labels) {
        metricFamily.addMetric(labels, value);
    }

    private int getTrackedConnectorJarCount(ConnectorPackageService connectorPackageService) {
        if (connectorPackageService != null) {
            return connectorPackageService.getTrackedConnectorJarCount();
        }
        return getConnectorJarRefCounters().size();
    }

    private long getTotalConnectorJarReferences(ConnectorPackageService connectorPackageService) {
        if (connectorPackageService != null) {
            return connectorPackageService.getTotalConnectorJarReferences();
        }
        long total = 0L;
        for (org.apache.seatunnel.engine.core.job.RefCount refCount :
                getConnectorJarRefCounters().values()) {
            if (refCount != null && refCount.getReferences() != null) {
                total += refCount.getReferences();
            }
        }
        return total;
    }

    private com.hazelcast.map.IMap<?, org.apache.seatunnel.engine.core.job.RefCount>
            getConnectorJarRefCounters() {
        return getNode()
                .hazelcastInstance
                .getMap(
                        org.apache.seatunnel.engine.common.Constant
                                .IMAP_CONNECTOR_JAR_REF_COUNTERS);
    }

    private void runSafely(String metricGroup, Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            getLogger(getClass())
                    .warning(
                            String.format(
                                    "Failed to collect logical state store metrics, group=%s, backend=%s",
                                    metricGroup, BACKEND),
                            e);
        }
    }
}

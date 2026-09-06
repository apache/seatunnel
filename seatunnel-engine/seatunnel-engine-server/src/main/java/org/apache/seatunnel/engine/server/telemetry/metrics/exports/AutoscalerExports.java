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
import org.apache.seatunnel.engine.server.autoscale.AutoscalerMetricsSnapshot;
import org.apache.seatunnel.engine.server.autoscale.AutoscalerView;
import org.apache.seatunnel.engine.server.autoscale.MetricStatus;
import org.apache.seatunnel.engine.server.autoscale.MetricValue;
import org.apache.seatunnel.engine.server.autoscale.ScalingAction;
import org.apache.seatunnel.engine.server.autoscale.ScalingRecommendation;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

/**
 * Prometheus projection of the latest published autoscaler view.
 *
 * <p>Scrapes are read-only: they do not collect signals, evaluate policy, or mutate recommendation
 * counters/history.
 */
public class AutoscalerExports extends AbstractCollector {

    private static final String STATUS = "status";

    public AutoscalerExports(Node node) {
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

        AutoscalerView view = coordinatorService.getAutoscalerView();
        String address = localAddress();
        collectState(mfs, address, view);

        AutoscalerMetricsSnapshot snapshot = view.getCurrentSnapshot();
        if (snapshot != null) {
            collectSnapshot(mfs, address, snapshot);
        }

        ScalingRecommendation recommendation = view.getLatestRecommendation();
        if (recommendation != null) {
            collectRecommendation(mfs, address, recommendation);
        }
        return mfs;
    }

    private void collectState(List<MetricFamilySamples> mfs, String address, AutoscalerView view) {
        GaugeMetricFamily enabled =
                new GaugeMetricFamily(
                        "seatunnel_autoscaler_enabled",
                        "Whether Zeta autoscaling recommendation is enabled",
                        clusterLabelNames(ADDRESS));
        enabled.addMetric(labelValues(address), booleanValue(view.isEnabled()));
        mfs.add(enabled);

        GaugeMetricFamily running =
                new GaugeMetricFamily(
                        "seatunnel_autoscaler_running",
                        "Whether Zeta autoscaling recommendation loop is running on the master",
                        clusterLabelNames(ADDRESS));
        running.addMetric(labelValues(address), booleanValue(view.isRunning()));
        mfs.add(running);

        addGauge(
                mfs,
                "seatunnel_autoscaler_current_master_epoch",
                "Current autoscaler master epoch",
                address,
                view.getCurrentMasterEpoch());
        addGauge(
                mfs,
                "seatunnel_autoscaler_next_generation",
                "Next autoscaler recommendation generation for the current master epoch",
                address,
                view.getNextGeneration());

        CounterMetricFamily recommendations =
                new CounterMetricFamily(
                        "seatunnel_autoscaler_recommendations_total",
                        "Autoscaler recommendations published by action",
                        clusterLabelNames(ADDRESS, "action"));
        for (ScalingAction action : ScalingAction.values()) {
            recommendations.addMetric(
                    labelValues(address, action.name()),
                    view.getRecommendationCounts().getOrDefault(action, 0L));
        }
        mfs.add(recommendations);

        GaugeMetricFamily stabilization =
                new GaugeMetricFamily(
                        "seatunnel_autoscaler_stabilization_seconds",
                        "Configured autoscaler stabilization window in seconds",
                        clusterLabelNames(ADDRESS, "direction"));
        stabilization.addMetric(
                labelValues(address, "scale_out"), view.getScaleOutStabilizationSeconds());
        stabilization.addMetric(
                labelValues(address, "scale_in"), view.getScaleInStabilizationSeconds());
        mfs.add(stabilization);
    }

    private void collectSnapshot(
            List<MetricFamilySamples> mfs, String address, AutoscalerMetricsSnapshot snapshot) {
        addGauge(
                mfs,
                "seatunnel_autoscaler_current_workers",
                "Current worker count observed by the autoscaler",
                address,
                snapshot.getCurrentWorkers());
        addGauge(
                mfs,
                "seatunnel_autoscaler_assigned_slots",
                "Assigned slot count observed by the autoscaler",
                address,
                snapshot.getAssignedSlots());
        addGauge(
                mfs,
                "seatunnel_autoscaler_unassigned_slots",
                "Unassigned slot count observed by the autoscaler",
                address,
                snapshot.getUnassignedSlots());
        addGauge(
                mfs,
                "seatunnel_autoscaler_pending_jobs",
                "Pending job count observed by the autoscaler",
                address,
                snapshot.getPendingJobCount());
        addGauge(
                mfs,
                "seatunnel_autoscaler_oldest_pending_job_duration_ms",
                "Oldest pending job duration observed by the autoscaler",
                address,
                snapshot.getOldestPendingDurationMillis());
        addGauge(
                mfs,
                "seatunnel_autoscaler_resource_shortages",
                "Resource shortage events observed by the autoscaler since the previous evaluation",
                address,
                snapshot.getResourceShortageCount());
        CounterMetricFamily shortageTotals =
                new CounterMetricFamily(
                        "seatunnel_autoscaler_resource_shortages_total",
                        "Cumulative resource shortage events observed by the autoscaler",
                        clusterLabelNames(ADDRESS, "strategy"));
        shortageTotals.addMetric(labelValues(address, "WAIT"), snapshot.getWaitShortageCount());
        shortageTotals.addMetric(labelValues(address, "REJECT"), snapshot.getRejectShortageCount());
        mfs.add(shortageTotals);

        collectMetricValue(
                mfs,
                "seatunnel_autoscaler_input_cpu_utilization",
                "Cluster CPU utilization observed by the autoscaler",
                address,
                snapshot.getCpu());
        collectMetricValue(
                mfs,
                "seatunnel_autoscaler_input_jvm_memory_utilization",
                "Cluster JVM memory utilization observed by the autoscaler",
                address,
                snapshot.getJvmMemory());
        collectSlotUtilization(
                mfs,
                "seatunnel_autoscaler_input_slot_utilization",
                "Fixed slot utilization observed by the autoscaler",
                address,
                snapshot.getFixedSlotUtilization());
        addGauge(
                mfs,
                "seatunnel_autoscaler_metrics_valid",
                "Whether autoscaler worker metrics are complete and valid for scale-in",
                address,
                booleanValue(snapshot.isScaleInMetricsValid()));

        GaugeMetricFamily sampleCounts =
                new GaugeMetricFamily(
                        "seatunnel_autoscaler_worker_samples",
                        "Worker metrics sample count observed by the autoscaler",
                        clusterLabelNames(ADDRESS, STATUS));
        sampleCounts.addMetric(labelValues(address, "total"), snapshot.getTotalWorkerSamples());
        sampleCounts.addMetric(labelValues(address, "valid"), snapshot.getValidWorkerSamples());
        sampleCounts.addMetric(labelValues(address, "missing"), snapshot.getMissingWorkerSamples());
        sampleCounts.addMetric(labelValues(address, "stale"), snapshot.getStaleWorkerSamples());
        sampleCounts.addMetric(labelValues(address, "future"), snapshot.getFutureWorkerSamples());
        mfs.add(sampleCounts);
    }

    private void collectRecommendation(
            List<MetricFamilySamples> mfs, String address, ScalingRecommendation recommendation) {
        GaugeMetricFamily workers =
                new GaugeMetricFamily(
                        "seatunnel_autoscaler_recommended_workers",
                        "Latest worker count recommended by the autoscaler",
                        clusterLabelNames(ADDRESS, "action", "recommendation_only"));
        workers.addMetric(
                labelValues(
                        address,
                        recommendation.getAction().name(),
                        Boolean.toString(recommendation.isRecommendationOnly())),
                recommendation.getRecommendedWorkers());
        mfs.add(workers);

        addGauge(
                mfs,
                "seatunnel_autoscaler_recommended_delta",
                "Latest recommended worker delta computed by the autoscaler",
                address,
                recommendation.getRecommendedWorkers() - recommendation.getCurrentWorkers());
        addGauge(
                mfs,
                "seatunnel_autoscaler_recommendation_generation",
                "Latest autoscaler recommendation generation for the current master epoch",
                address,
                recommendation.getGeneration());

        GaugeMetricFamily info =
                new GaugeMetricFamily(
                        "seatunnel_autoscaler_info",
                        "Current autoscaler recommendation information",
                        clusterLabelNames(ADDRESS, "slot_mode", "action"));
        info.addMetric(
                labelValues(
                        address,
                        recommendation.getSnapshot().getSlotMode().name(),
                        recommendation.getAction().name()),
                1.0d);
        mfs.add(info);
    }

    private void collectMetricValue(
            List<MetricFamilySamples> mfs,
            String name,
            String help,
            String address,
            MetricValue value) {
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(name, help, clusterLabelNames(ADDRESS, STATUS));
        metricFamily.addMetric(
                labelValues(address, value.getStatus().name()),
                value.isValid() ? value.getValue() : Double.NaN);
        mfs.add(metricFamily);
    }

    private void collectSlotUtilization(
            List<MetricFamilySamples> mfs,
            String name,
            String help,
            String address,
            MetricValue value) {
        if (value.getStatus() != MetricStatus.VALID) {
            return;
        }
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(name, help, clusterLabelNames(ADDRESS, STATUS));
        metricFamily.addMetric(labelValues(address, value.getStatus().name()), value.getValue());
        mfs.add(metricFamily);
    }

    private void addGauge(
            List<MetricFamilySamples> mfs, String name, String help, String address, double value) {
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(name, help, clusterLabelNames(ADDRESS));
        metricFamily.addMetric(labelValues(address), value);
        mfs.add(metricFamily);
    }

    private double booleanValue(boolean value) {
        return value ? 1.0d : 0.0d;
    }
}

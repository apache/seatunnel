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

import org.apache.seatunnel.engine.server.observability.cluster.ClusterObservabilityService;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.partition.PartitionService;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ClusterMetricExports extends AbstractCollector {

    public ClusterMetricExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();

        // cluster_info
        clusterInfo(mfs);
        // cluster_time
        clusterTime(mfs);
        // instance count
        nodeCount(mfs);
        // Expose cluster-level operator metrics from the active master only to
        // avoid duplicate series from every member.
        if (isMaster()) {
            // operator-facing cluster health
            clusterHealth(mfs);
            // topology change counters and timestamps
            clusterTopology(mfs);
        }

        return mfs;
    }

    private void clusterTime(final List<MetricFamilySamples> mfs) {
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(
                        "cluster_time",
                        "Cluster start time",
                        clusterLabelNames("hazelcastVersion"));
        metricFamily.addMetric(
                labelValues(getClusterService().getClusterVersion().toString()),
                getClusterService().getClusterTime());
        mfs.add(metricFamily);
    }

    private void clusterInfo(final List<MetricFamilySamples> mfs) {
        // Snapshot once to avoid TOCTOU race during master election.
        Address masterAddr = getClusterService().getMasterAddress();
        if (masterAddr == null) {
            return;
        }
        // Keep the historical label format compatible with previous IP:port output.
        String masterIpPort;
        try {
            masterIpPort =
                    masterAddr.getInetAddress().getHostAddress() + ":" + masterAddr.getPort();
        } catch (UnknownHostException e) {
            getLogger(ClusterMetricExports.class)
                    .warning("Skip cluster_info metric: unable to resolve master address", e);
            return;
        }
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(
                        "cluster_info",
                        "Cluster info",
                        clusterLabelNames("hazelcastVersion", "master"));
        List<String> labelValues =
                labelValues(getClusterService().getClusterVersion().toString(), masterIpPort);
        metricFamily.addMetric(labelValues, 1.0);
        mfs.add(metricFamily);
    }

    private void nodeCount(final List<MetricFamilySamples> mfs) {
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(
                        "node_count", "Cluster node total count ", clusterLabelNames());
        metricFamily.addMetric(labelValues(), getClusterService().getMemberImpls().size());
        mfs.add(metricFamily);
    }

    private void clusterHealth(final List<MetricFamilySamples> mfs) {
        GaugeMetricFamily clusterSafeMetricFamily =
                new GaugeMetricFamily(
                        "seatunnel_engine_cluster_safe",
                        "Whether the SeaTunnel Engine cluster partition state is currently safe",
                        clusterLabelNames());
        clusterSafeMetricFamily.addMetric(labelValues(), resolveClusterSafe() ? 1 : 0);
        mfs.add(clusterSafeMetricFamily);

        GaugeMetricFamily memberCountMetricFamily =
                new GaugeMetricFamily(
                        "seatunnel_engine_cluster_member_count",
                        "The current SeaTunnel Engine cluster member count",
                        clusterLabelNames());
        memberCountMetricFamily.addMetric(
                labelValues(), getClusterService().getMemberImpls().size());
        mfs.add(memberCountMetricFamily);

        GaugeMetricFamily migrationMetricFamily =
                new GaugeMetricFamily(
                        "seatunnel_engine_cluster_partition_migration_in_progress",
                        "Whether SeaTunnel Engine cluster partition migration is currently in progress",
                        clusterLabelNames());
        migrationMetricFamily.addMetric(labelValues(), hasOngoingMigration() ? 1 : 0);
        mfs.add(migrationMetricFamily);
    }

    private void clusterTopology(final List<MetricFamilySamples> mfs) {
        if (getServer() == null) {
            return;
        }
        ClusterObservabilityService clusterObservabilityService =
                getServer().getClusterObservabilityService();
        if (clusterObservabilityService == null) {
            return;
        }
        ClusterObservabilityService.ClusterObservabilitySnapshot snapshot =
                clusterObservabilityService.snapshot();

        CounterMetricFamily masterChangeTotalMetricFamily =
                new CounterMetricFamily(
                        "seatunnel_engine_cluster_master_change",
                        "The total number of observed SeaTunnel Engine master changes",
                        clusterLabelNames());
        masterChangeTotalMetricFamily.addMetric(labelValues(), snapshot.getMasterChangeTotal());
        mfs.add(masterChangeTotalMetricFamily);

        CounterMetricFamily memberJoinTotalMetricFamily =
                new CounterMetricFamily(
                        "seatunnel_engine_cluster_member_join",
                        "The total number of observed SeaTunnel Engine member joins",
                        clusterLabelNames());
        memberJoinTotalMetricFamily.addMetric(labelValues(), snapshot.getMemberJoinTotal());
        mfs.add(memberJoinTotalMetricFamily);

        CounterMetricFamily memberLeaveTotalMetricFamily =
                new CounterMetricFamily(
                        "seatunnel_engine_cluster_member_leave",
                        "The total number of observed SeaTunnel Engine member leaves",
                        clusterLabelNames());
        memberLeaveTotalMetricFamily.addMetric(labelValues(), snapshot.getMemberLeaveTotal());
        mfs.add(memberLeaveTotalMetricFamily);

        GaugeMetricFamily masterChangeTimestampMetricFamily =
                new GaugeMetricFamily(
                        "seatunnel_engine_cluster_last_master_change_timestamp_ms",
                        "The timestamp in milliseconds of the most recent SeaTunnel Engine master change",
                        clusterLabelNames());
        masterChangeTimestampMetricFamily.addMetric(
                labelValues(), snapshot.getLastMasterChangeTimestampMs());
        mfs.add(masterChangeTimestampMetricFamily);

        GaugeMetricFamily memberJoinTimestampMetricFamily =
                new GaugeMetricFamily(
                        "seatunnel_engine_cluster_last_member_join_timestamp_ms",
                        "The timestamp in milliseconds of the most recent SeaTunnel Engine member join",
                        clusterLabelNames());
        memberJoinTimestampMetricFamily.addMetric(
                labelValues(), snapshot.getLastMemberJoinTimestampMs());
        mfs.add(memberJoinTimestampMetricFamily);

        GaugeMetricFamily memberLeaveTimestampMetricFamily =
                new GaugeMetricFamily(
                        "seatunnel_engine_cluster_last_member_leave_timestamp_ms",
                        "The timestamp in milliseconds of the most recent SeaTunnel Engine member leave",
                        clusterLabelNames());
        memberLeaveTimestampMetricFamily.addMetric(
                labelValues(), snapshot.getLastMemberLeaveTimestampMs());
        mfs.add(memberLeaveTimestampMetricFamily);
    }

    private boolean resolveClusterSafe() {
        HazelcastInstanceImpl hazelcastInstance = getNode().hazelcastInstance;
        PartitionService partitionService =
                hazelcastInstance == null ? null : hazelcastInstance.getPartitionService();
        return partitionService != null && partitionService.isClusterSafe();
    }

    private boolean hasOngoingMigration() {
        return getNode().getPartitionService() != null
                && getNode().getPartitionService().hasOnGoingMigration();
    }
}

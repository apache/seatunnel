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

import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
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
}

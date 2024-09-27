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

package org.apache.seatunnel.engine.server.telemetry.metrics;

import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.google.common.collect.Lists;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCollector extends Collector {

    protected static String CLUSTER = "cluster";
    protected static String ADDRESS = "address";

    protected Node node;

    public AbstractCollector(final Node node) {
        this.node = node;
    }

    protected Node getNode() {
        return node;
    }

    protected ILogger getLogger(Class clazz) {
        return getNode().getLogger(clazz);
    }

    protected boolean isMaster() {
        return getNode().isMaster();
    }

    protected MemberImpl getLocalMember() {
        return getNode().nodeEngine.getLocalMember();
    }

    protected SeaTunnelServer getServer() {
        return getNode().getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    protected CoordinatorService getCoordinatorService() {
        return getServer().getCoordinatorService();
    }

    protected ManagementService getManagementService() {
        return getNode().hazelcastInstance.getManagementService();
    }

    protected ClusterService getClusterService() {
        return getNode().getClusterService();
    }

    protected String localAddress() {
        return getLocalMember().getInetAddress().getHostAddress()
                + ":"
                + getLocalMember().getPort();
    }

    protected String masterAddress() throws UnknownHostException {
        return getClusterService().getMasterAddress().getInetAddress().getHostAddress()
                + ":"
                + getClusterService().getMasterAddress().getPort();
    }

    protected String getClusterName() {
        return getNode().getConfig().getClusterName();
    }

    protected List<String> labelValues(String... values) {
        List<String> labelValues = new ArrayList<>();
        labelValues.add(getClusterName());
        if (values != null) {
            labelValues.addAll(Lists.newArrayList(values));
        }
        return labelValues;
    }

    protected List<String> clusterLabelNames(String... labels) {
        List<String> labelNames = new ArrayList<>();
        labelNames.add(CLUSTER);
        if (labels != null) {
            labelNames.addAll(Lists.newArrayList(labels));
        }
        return labelNames;
    }

    protected void longMetric(
            GaugeMetricFamily metricFamily, long count, List<String> labelValues) {
        metricFamily.addMetric(labelValues, count);
    }

    protected void intMetric(GaugeMetricFamily metricFamily, int count, List<String> labelValues) {
        metricFamily.addMetric(labelValues, count);
    }
}

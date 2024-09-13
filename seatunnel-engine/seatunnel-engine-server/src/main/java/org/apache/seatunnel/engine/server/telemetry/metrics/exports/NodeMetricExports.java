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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.jmx.InstanceMBean;
import com.hazelcast.internal.jmx.PartitionServiceMBean;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

public class NodeMetricExports extends AbstractCollector {

    public NodeMetricExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();
        // instance state
        nodeState(mfs);

        InstanceMBean instanceMBean = getManagementService().getInstanceMBean();
        if (instanceMBean == null) {
            return mfs;
        }

        // node hazelcast executor
        String address = localAddress();
        List<String> labelNames = clusterLabelNames(ADDRESS, "type");
        GaugeMetricFamily isShutdownMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_isShutdown",
                        "The hazelcast executor isShutdown of seatunnel cluster node",
                        labelNames);
        GaugeMetricFamily isTerminatedMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_isTerminated",
                        "The hazelcast executor isTerminated of seatunnel cluster node",
                        labelNames);

        GaugeMetricFamily maxPoolSizeMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_maxPoolSize",
                        "The hazelcast executor maxPoolSize of seatunnel cluster node",
                        labelNames);

        GaugeMetricFamily poolSizeMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_poolSize",
                        "The hazelcast executor poolSize of seatunnel cluster node",
                        labelNames);

        GaugeMetricFamily queueRemainingCapacityMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_queueRemainingCapacity",
                        "The hazelcast executor queueRemainingCapacity of seatunnel cluster ",
                        labelNames);

        GaugeMetricFamily queueSizeMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_queueSize",
                        "The hazelcast executor queueSize of seatunnel cluster node",
                        labelNames);

        GaugeMetricFamily executedCountMetricFamily =
                new GaugeMetricFamily(
                        "hazelcast_executor_executedCount",
                        "The hazelcast executor executedCount of seatunnel cluster node",
                        labelNames);

        List<String> asyncValues = labelValues(address, "async");
        List<String> clientBlockingValues = labelValues(address, "clientBlocking");
        List<String> clientExecutorValues = labelValues(address, "client");
        List<String> clientQueryValues = labelValues(address, "clientQuery");
        List<String> ioValues = labelValues(address, "io");
        List<String> offloadableValues = labelValues(address, "offloadable");
        List<String> scheduledValues = labelValues(address, "scheduled");
        List<String> systemValues = labelValues(address, "system");

        // Executor executedCount
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getAsyncExecutorMBean().getExecutedCount(),
                asyncValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getClientExecutorMBean().getExecutedCount(),
                clientExecutorValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().getExecutedCount(),
                clientBlockingValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().getExecutedCount(),
                clientQueryValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getIoExecutorMBean().getExecutedCount(),
                ioValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().getExecutedCount(),
                offloadableValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getScheduledExecutorMBean().getExecutedCount(),
                scheduledValues);
        longMetric(
                executedCountMetricFamily,
                instanceMBean.getSystemExecutorMBean().getExecutedCount(),
                systemValues);
        mfs.add(executedCountMetricFamily);

        // Executor isShutdown
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getAsyncExecutorMBean().isShutdown() ? 1 : 0,
                asyncValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getClientExecutorMBean().isShutdown() ? 1 : 0,
                clientExecutorValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().isShutdown() ? 1 : 0,
                clientBlockingValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().isShutdown() ? 1 : 0,
                clientQueryValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getIoExecutorMBean().isShutdown() ? 1 : 0,
                ioValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().isShutdown() ? 1 : 0,
                offloadableValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getScheduledExecutorMBean().isShutdown() ? 1 : 0,
                scheduledValues);
        intMetric(
                isShutdownMetricFamily,
                instanceMBean.getSystemExecutorMBean().isShutdown() ? 1 : 0,
                systemValues);
        mfs.add(isShutdownMetricFamily);

        // Executor isTerminated
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getAsyncExecutorMBean().isTerminated() ? 1 : 0,
                asyncValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getClientExecutorMBean().isTerminated() ? 1 : 0,
                clientExecutorValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().isTerminated() ? 1 : 0,
                clientBlockingValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().isTerminated() ? 1 : 0,
                clientQueryValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getIoExecutorMBean().isTerminated() ? 1 : 0,
                ioValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().isTerminated() ? 1 : 0,
                offloadableValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getScheduledExecutorMBean().isTerminated() ? 1 : 0,
                scheduledValues);
        intMetric(
                isTerminatedMetricFamily,
                instanceMBean.getSystemExecutorMBean().isTerminated() ? 1 : 0,
                systemValues);
        mfs.add(isTerminatedMetricFamily);

        // Executor maxPoolSize
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getAsyncExecutorMBean().maxPoolSize(),
                asyncValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getClientExecutorMBean().maxPoolSize(),
                clientExecutorValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().maxPoolSize(),
                clientBlockingValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().maxPoolSize(),
                clientQueryValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getIoExecutorMBean().maxPoolSize(),
                ioValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().maxPoolSize(),
                offloadableValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getScheduledExecutorMBean().maxPoolSize(),
                scheduledValues);
        intMetric(
                maxPoolSizeMetricFamily,
                instanceMBean.getSystemExecutorMBean().maxPoolSize(),
                systemValues);
        mfs.add(maxPoolSizeMetricFamily);

        // Executor poolSize
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getAsyncExecutorMBean().poolSize(),
                asyncValues);
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getClientExecutorMBean().poolSize(),
                clientExecutorValues);
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().poolSize(),
                clientBlockingValues);
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().poolSize(),
                clientQueryValues);
        intMetric(poolSizeMetricFamily, instanceMBean.getIoExecutorMBean().poolSize(), ioValues);
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().poolSize(),
                offloadableValues);
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getScheduledExecutorMBean().poolSize(),
                scheduledValues);
        intMetric(
                poolSizeMetricFamily,
                instanceMBean.getSystemExecutorMBean().poolSize(),
                systemValues);
        mfs.add(poolSizeMetricFamily);

        // Executor queueRemainingCapacity
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getAsyncExecutorMBean().queueRemainingCapacity(),
                asyncValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getClientExecutorMBean().queueRemainingCapacity(),
                clientExecutorValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().queueRemainingCapacity(),
                clientBlockingValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().queueRemainingCapacity(),
                clientQueryValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getIoExecutorMBean().queueRemainingCapacity(),
                ioValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().queueRemainingCapacity(),
                offloadableValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getScheduledExecutorMBean().queueRemainingCapacity(),
                scheduledValues);
        intMetric(
                queueRemainingCapacityMetricFamily,
                instanceMBean.getSystemExecutorMBean().queueRemainingCapacity(),
                systemValues);
        mfs.add(queueRemainingCapacityMetricFamily);

        // Executor queueSize
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getAsyncExecutorMBean().queueSize(),
                asyncValues);
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getClientExecutorMBean().queueSize(),
                clientExecutorValues);
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getClientBlockingExecutorMBean().queueSize(),
                clientBlockingValues);
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getClientQueryExecutorMBean().queueSize(),
                clientQueryValues);
        intMetric(queueSizeMetricFamily, instanceMBean.getIoExecutorMBean().queueSize(), ioValues);
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getOffloadableExecutorMBean().queueSize(),
                offloadableValues);
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getScheduledExecutorMBean().queueSize(),
                scheduledValues);
        intMetric(
                queueSizeMetricFamily,
                instanceMBean.getSystemExecutorMBean().queueSize(),
                systemValues);
        mfs.add(queueSizeMetricFamily);

        // partition metric
        partitionMetric(instanceMBean.getPartitionServiceMBean(), mfs, address);

        return mfs;
    }

    private void partitionMetric(
            PartitionServiceMBean partitionServiceMBean,
            List<MetricFamilySamples> mfs,
            String address) {
        List<String> labelNames = clusterLabelNames(ADDRESS);

        GaugeMetricFamily partitionPartitionCount =
                new GaugeMetricFamily(
                        "hazelcast_partition_partitionCount",
                        "The partitionCount of seatunnel cluster node",
                        labelNames);
        intMetric(
                partitionPartitionCount,
                partitionServiceMBean.getPartitionCount(),
                labelValues(address));
        mfs.add(partitionPartitionCount);

        GaugeMetricFamily partitionActivePartition =
                new GaugeMetricFamily(
                        "hazelcast_partition_activePartition",
                        "The activePartition of seatunnel cluster node",
                        labelNames);
        intMetric(
                partitionActivePartition,
                partitionServiceMBean.getActivePartitionCount(),
                labelValues(address));
        mfs.add(partitionActivePartition);

        GaugeMetricFamily partitionIsClusterSafe =
                new GaugeMetricFamily(
                        "hazelcast_partition_isClusterSafe",
                        "Whether is cluster safe of partition",
                        labelNames);
        intMetric(
                partitionIsClusterSafe,
                partitionServiceMBean.isClusterSafe() ? 1 : 0,
                labelValues(address));
        mfs.add(partitionIsClusterSafe);

        GaugeMetricFamily partitionIsLocalMemberSafe =
                new GaugeMetricFamily(
                        "hazelcast_partition_isLocalMemberSafe",
                        "Whether is local member safe of partition",
                        labelNames);
        intMetric(
                partitionIsLocalMemberSafe,
                partitionServiceMBean.isLocalMemberSafe() ? 1 : 0,
                labelValues(address));
        mfs.add(partitionIsLocalMemberSafe);
    }

    private void nodeState(List<MetricFamilySamples> mfs) {
        GaugeMetricFamily metricFamily =
                new GaugeMetricFamily(
                        "node_state",
                        "Whether is up of seatunnel node ",
                        clusterLabelNames(ADDRESS));
        String address = localAddress();
        List<String> labelValues = labelValues(address);
        metricFamily.addMetric(labelValues, 1);
        mfs.add(metricFamily);
    }
}

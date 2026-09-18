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
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

public class JobThreadPoolStatusExports extends AbstractCollector {

    private static String HELP = "The %s of the SeaTunnel coordinator %s thread pool";

    public JobThreadPoolStatusExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();
        // Only report metrics when the local node is master and in READY state.
        if (isMaster() && isCoordinatorReady()) {
            addThreadPoolMetrics(
                    mfs, getServer().getThreadPoolStatusMetrics(), "job_thread_pool_", "admission");
            addThreadPoolMetrics(
                    mfs,
                    getServer().getLifecycleThreadPoolStatusMetrics(),
                    "job_lifecycle_thread_pool_",
                    "lifecycle");
        }
        return mfs;
    }

    private void addThreadPoolMetrics(
            List<MetricFamilySamples> mfs,
            ThreadPoolStatus threadPoolStatusMetrics,
            String prefix,
            String pool) {
        List<String> labelNames = clusterLabelNames(ADDRESS, "type");

        GaugeMetricFamily activeCount =
                new GaugeMetricFamily(
                        prefix + "activeCount",
                        String.format(HELP, "activeCount", pool),
                        labelNames);
        activeCount.addMetric(
                labelValues(localAddress(), "activeCount"),
                threadPoolStatusMetrics.getActiveCount());
        mfs.add(activeCount);

        CounterMetricFamily completedTask =
                new CounterMetricFamily(
                        prefix + "completedTask",
                        String.format(HELP, "completedTask", pool),
                        labelNames);
        completedTask.addMetric(
                labelValues(localAddress(), "completedTask"),
                threadPoolStatusMetrics.getCompletedTaskCount());
        mfs.add(completedTask);

        GaugeMetricFamily corePoolSize =
                new GaugeMetricFamily(
                        prefix + "corePoolSize",
                        String.format(HELP, "corePoolSize", pool),
                        labelNames);
        corePoolSize.addMetric(
                labelValues(localAddress(), "corePoolSize"),
                threadPoolStatusMetrics.getCorePoolSize());
        mfs.add(corePoolSize);

        GaugeMetricFamily maximumPoolSize =
                new GaugeMetricFamily(
                        prefix + "maximumPoolSize",
                        String.format(HELP, "maximumPoolSize", pool),
                        labelNames);
        maximumPoolSize.addMetric(
                labelValues(localAddress(), "maximumPoolSize"),
                threadPoolStatusMetrics.getMaximumPoolSize());
        mfs.add(maximumPoolSize);

        GaugeMetricFamily poolSize =
                new GaugeMetricFamily(
                        prefix + "poolSize", String.format(HELP, "poolSize", pool), labelNames);
        poolSize.addMetric(
                labelValues(localAddress(), "poolSize"), threadPoolStatusMetrics.getPoolSize());
        mfs.add(poolSize);

        CounterMetricFamily taskCount =
                new CounterMetricFamily(
                        prefix + "task", String.format(HELP, "taskCount", pool), labelNames);
        taskCount.addMetric(
                labelValues(localAddress(), "taskCount"), threadPoolStatusMetrics.getTaskCount());
        mfs.add(taskCount);

        GaugeMetricFamily queueTaskCount =
                new GaugeMetricFamily(
                        prefix + "queueTaskCount",
                        String.format(HELP, "queueTaskCount", pool),
                        labelNames);
        queueTaskCount.addMetric(
                labelValues(localAddress(), "queueTaskCount"),
                threadPoolStatusMetrics.getQueueTaskCount());
        mfs.add(queueTaskCount);

        CounterMetricFamily rejectedTaskCount =
                new CounterMetricFamily(
                        prefix + "rejection",
                        String.format(HELP, "rejectionCount", pool),
                        labelNames);
        rejectedTaskCount.addMetric(
                labelValues(localAddress(), "rejectionCount"),
                threadPoolStatusMetrics.getRejectionCount());
        mfs.add(rejectedTaskCount);
    }
}

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
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.JobCounter;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

public class JobMetricExports extends AbstractCollector {

    public JobMetricExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();
        // Only the master can get job metrics
        if (isMaster()) {
            CoordinatorService coordinatorService = getCoordinatorService();
            JobCounter jobCountMetrics = coordinatorService.getJobCountMetrics();

            GaugeMetricFamily metricFamily =
                    new GaugeMetricFamily(
                            "job_count",
                            "All job counts of seatunnel cluster ",
                            clusterLabelNames("type"));

            metricFamily.addMetric(labelValues("canceled"), jobCountMetrics.getCanceledJobCount());
            metricFamily.addMetric(
                    labelValues("cancelling"), jobCountMetrics.getCancellingJobCount());
            metricFamily.addMetric(labelValues("created"), jobCountMetrics.getCreatedJobCount());
            metricFamily.addMetric(labelValues("failed"), jobCountMetrics.getFailedJobCount());
            metricFamily.addMetric(labelValues("failing"), jobCountMetrics.getFailingJobCount());
            metricFamily.addMetric(labelValues("finished"), jobCountMetrics.getFinishedJobCount());
            metricFamily.addMetric(labelValues("running"), jobCountMetrics.getRunningJobCount());
            metricFamily.addMetric(
                    labelValues("scheduled"), jobCountMetrics.getScheduledJobCount());

            mfs.add(metricFamily);
        }
        return mfs;
    }
}

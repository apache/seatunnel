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

import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.List;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;
import org.apache.seatunnel.engine.server.telemetry.metrics.ExportsInstance;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.JobCounter;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

public class ThreadPoolStatusExports extends AbstractCollector {

    public ThreadPoolStatusExports(ExportsInstance exportsInstance) {
        super(exportsInstance);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();
        // Only the master can get job metrics
        if (isMaster()) {
            CoordinatorService coordinatorService = getCoordinatorService();
            ThreadPoolStatus threadPoolStatusMetrics = coordinatorService.getThreadPoolStatusMetrics();
            // TODO
            mfs.add(new GaugeMetricFamily("jobMetric", "jobMetric", 1));
        }
        return mfs;
    }
}


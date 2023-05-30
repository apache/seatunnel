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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;

public abstract class CoordinatorTask extends AbstractTask {

    private static final long serialVersionUID = -3957168748281681077L;

    private SeaTunnelMetricsContext metricsContext;

    public CoordinatorTask(long jobID, TaskLocation taskID) {
        super(jobID, taskID);
    }

    @Override
    public void init() throws Exception {
        super.init();
        metricsContext = getExecutionContext().getOrCreateMetricsContext(taskLocation);
    }

    @Override
    public SeaTunnelMetricsContext getMetricsContext() {
        return metricsContext;
    }

    @Override
    public void provideDynamicMetrics(
            MetricDescriptor descriptor, MetricsCollectionContext context) {
        if (null != metricsContext) {
            metricsContext.provideDynamicMetrics(
                    descriptor
                            .copy()
                            .withTag(MetricTags.TASK_NAME, this.getClass().getSimpleName()),
                    context);
        }
    }
}

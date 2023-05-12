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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.logging.ILogger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class JobMetricsCollector implements MetricsCollector {

    private final List<String> taskGroupLocationStrs;
    private final MetricsCompressor compressor;
    private final ILogger logger;
    private final UnaryOperator<MetricDescriptor> addPrefixFn;

    public JobMetricsCollector(TaskGroupLocation taskGroupLocation, Member member, ILogger logger) {
        Objects.requireNonNull(member, "member");
        this.logger = Objects.requireNonNull(logger, "logger");

        this.taskGroupLocationStrs = Collections.singletonList(taskGroupLocation.toString());
        this.addPrefixFn = JobMetricsUtil.addMemberPrefixFn(member);
        this.compressor = new MetricsCompressor();
    }

    public JobMetricsCollector(
            List<TaskGroupLocation> taskGroupLocations, Member member, ILogger logger) {
        Objects.requireNonNull(member, "member");
        this.logger = Objects.requireNonNull(logger, "logger");

        this.taskGroupLocationStrs =
                taskGroupLocations.stream()
                        .map(TaskGroupLocation::toString)
                        .collect(Collectors.toList());
        this.addPrefixFn = JobMetricsUtil.addMemberPrefixFn(member);
        this.compressor = new MetricsCompressor();
    }

    @Override
    public void collectLong(MetricDescriptor descriptor, long value) {
        String taskGroupLocationStr =
                JobMetricsUtil.getTaskGroupLocationFromMetricsDescriptor(descriptor);
        if (taskGroupLocationStrs.contains(taskGroupLocationStr)) {
            compressor.addLong(addPrefixFn.apply(descriptor), value);
        }
    }

    @Override
    public void collectDouble(MetricDescriptor descriptor, double value) {
        String taskGroupLocationStr =
                JobMetricsUtil.getTaskGroupLocationFromMetricsDescriptor(descriptor);
        if (taskGroupLocationStrs.contains(taskGroupLocationStr)) {
            compressor.addDouble(addPrefixFn.apply(descriptor), value);
        }
    }

    @Override
    public void collectException(MetricDescriptor descriptor, Exception e) {
        String taskGroupLocationStr =
                JobMetricsUtil.getTaskGroupLocationFromMetricsDescriptor(descriptor);
        if (taskGroupLocationStrs.contains(taskGroupLocationStr)) {
            logger.warning("Exception when rendering job metrics: " + e, e);
        }
    }

    @Override
    public void collectNoValue(MetricDescriptor descriptor) {}

    public RawJobMetrics getMetrics() {
        return RawJobMetrics.of(compressor.getBlobAndReset());
    }
}

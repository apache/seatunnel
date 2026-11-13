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

import org.apache.seatunnel.api.common.metrics.MetricTags;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier;

import java.util.HashMap;
import java.util.Map;

public final class MetricsTestUtils {

    private MetricsTestUtils() {}

    public static Map<String, Long> exportDynamicCounters(
            SeaTunnelMetricsContext context, long jobId) {
        Map<String, Long> map = new HashMap<>();
        MetricDescriptor tagger =
                DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER
                        .get()
                        .withTag(MetricTags.JOB_ID, String.valueOf(jobId));
        context.provideDynamicMetrics(
                tagger,
                new MetricsCollectionContext() {
                    @Override
                    public void collect(
                            MetricDescriptor descriptor,
                            String name,
                            ProbeLevel probeLevel,
                            ProbeUnit probeUnit,
                            long value) {
                        map.put(name, value);
                    }

                    @Override
                    public void collect(MetricDescriptor descriptor, Object value) {}

                    @Override
                    public void collect(
                            MetricDescriptor descriptor,
                            String name,
                            ProbeLevel probeLevel,
                            ProbeUnit probeUnit,
                            double value) {}

                    @Override
                    public void collect(MetricDescriptor descriptor, long value) {}

                    @Override
                    public void collect(MetricDescriptor descriptor, double value) {}
                });
        return map;
    }
}

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

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricTags;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier;

import java.util.HashMap;
import java.util.Map;

public class SeaTunnelMetricsContextTest {

    private static final long JOB_ID = 1001L;
    private static final String QUEUE_SUFFIX = "#-2";
    private static final String EXTENDED_QUEUE_SIZE_NAME = "IntermediateQueueSizeExtended";
    private static final String EXTENDED_QUEUE_CAPACITY_NAME = "IntermediateQueueCapacityConfig";

    @Test
    public void testCleanIntermediateQueueMetricsRemovesStaleQueueMetricsAndPreservesUnrelated() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();

        // 1. Register transient queue metrics
        context.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE).set(50);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + QUEUE_SUFFIX).set(50);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + QUEUE_SUFFIX).set(1000);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + QUEUE_SUFFIX).set(2048);

        // 2. Register unrelated cumulative metrics
        context.counter(MetricNames.SOURCE_RECEIVED_COUNT).set(500);
        context.counter(MetricNames.SOURCE_RECEIVED_BYTES).set(10240);
        context.counter(MetricNames.SINK_WRITE_COUNT).set(200);

        // 3. Register similarly prefixed metrics
        context.counter(EXTENDED_QUEUE_SIZE_NAME).set(10);
        context.counter(EXTENDED_QUEUE_CAPACITY_NAME).set(20);

        // 4. Register flush signal counters
        context.counter(MetricNames.FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL).set(30);
        context.counter(MetricNames.FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL).set(5);

        // 5. Perform cleanup
        context.cleanIntermediateQueueMetrics();

        // 6. Export dynamic metrics
        Map<String, Long> exported = exportDynamicCounters(context, JOB_ID);

        // Verify transient queue metrics are absent
        Assertions.assertFalse(
                exported.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE),
                "Base IntermediateQueueSize should be removed");
        Assertions.assertFalse(
                exported.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE + QUEUE_SUFFIX),
                "IntermediateQueueSize#-2 should be removed");
        Assertions.assertFalse(
                exported.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + QUEUE_SUFFIX),
                "IntermediateQueuePutBlockedNs#-2 should be removed");
        Assertions.assertFalse(
                exported.containsKey(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + QUEUE_SUFFIX),
                "IntermediateQueueCapacity#-2 should be removed");

        // Verify unrelated metrics retain exact values
        Assertions.assertEquals(500L, exported.get(MetricNames.SOURCE_RECEIVED_COUNT));
        Assertions.assertEquals(10240L, exported.get(MetricNames.SOURCE_RECEIVED_BYTES));
        Assertions.assertEquals(200L, exported.get(MetricNames.SINK_WRITE_COUNT));

        // Verify similarly prefixed metric names remain
        Assertions.assertEquals(10L, exported.get(EXTENDED_QUEUE_SIZE_NAME));
        Assertions.assertEquals(20L, exported.get(EXTENDED_QUEUE_CAPACITY_NAME));

        // Verify flush signal counters remain
        Assertions.assertEquals(30L, exported.get(MetricNames.FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL));
        Assertions.assertEquals(5L, exported.get(MetricNames.FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL));

        // Verify preserved counters can still be incremented and read
        context.counter(MetricNames.SOURCE_RECEIVED_COUNT).inc(10);
        context.counter(EXTENDED_QUEUE_SIZE_NAME).inc(5);
        context.counter(MetricNames.FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL).inc(1);

        Assertions.assertEquals(
                510L, context.counter(MetricNames.SOURCE_RECEIVED_COUNT).getCount());
        Assertions.assertEquals(15L, context.counter(EXTENDED_QUEUE_SIZE_NAME).getCount());
        Assertions.assertEquals(
                31L, context.counter(MetricNames.FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL).getCount());
    }

    @Test
    public void testCleanIntermediateQueueMetricsIsIdempotent() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();

        // Register transient queue metrics
        context.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE).set(50);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + QUEUE_SUFFIX).set(50);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + QUEUE_SUFFIX).set(1000);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + QUEUE_SUFFIX).set(2048);

        // Register preserved metrics
        context.counter(MetricNames.SOURCE_RECEIVED_COUNT).set(500);
        context.counter(MetricNames.SOURCE_RECEIVED_BYTES).set(10240);
        context.counter(MetricNames.SINK_WRITE_COUNT).set(200);
        context.counter(EXTENDED_QUEUE_SIZE_NAME).set(10);
        context.counter(EXTENDED_QUEUE_CAPACITY_NAME).set(20);
        context.counter(MetricNames.FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL).set(30);
        context.counter(MetricNames.FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL).set(5);

        // First clean
        context.cleanIntermediateQueueMetrics();
        Map<String, Long> firstCleanExported = exportDynamicCounters(context, JOB_ID);

        // Second clean (idempotent call)
        context.cleanIntermediateQueueMetrics();
        Map<String, Long> secondCleanExported = exportDynamicCounters(context, JOB_ID);

        Assertions.assertEquals(
                firstCleanExported,
                secondCleanExported,
                "Calling cleanIntermediateQueueMetrics() repeatedly must produce the identical state");

        // Verify new transient queue metrics can be registered after cleanup
        context.counter(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + QUEUE_SUFFIX).set(4096);
        context.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + QUEUE_SUFFIX).set(15);

        Map<String, Long> reRegisteredExported = exportDynamicCounters(context, JOB_ID);
        Assertions.assertEquals(
                4096L,
                reRegisteredExported.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + QUEUE_SUFFIX));
        Assertions.assertEquals(
                15L, reRegisteredExported.get(MetricNames.INTERMEDIATE_QUEUE_SIZE + QUEUE_SUFFIX));

        // Clean again removes newly registered queue metrics while keeping preserved metrics intact
        context.cleanIntermediateQueueMetrics();
        Map<String, Long> cleanedAgainExported = exportDynamicCounters(context, JOB_ID);

        Assertions.assertFalse(
                cleanedAgainExported.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_CAPACITY + QUEUE_SUFFIX));
        Assertions.assertFalse(
                cleanedAgainExported.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_SIZE + QUEUE_SUFFIX));
        Assertions.assertEquals(
                firstCleanExported,
                cleanedAgainExported,
                "State after cleaning re-registered transient queue metrics must match initial clean state");
    }

    private static Map<String, Long> exportDynamicCounters(
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

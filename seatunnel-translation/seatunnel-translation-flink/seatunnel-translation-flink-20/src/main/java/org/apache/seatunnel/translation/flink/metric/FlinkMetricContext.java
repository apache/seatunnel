/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class FlinkMetricContext implements MetricsContext {

    private final MetricGroup metricGroup;
    private final StreamingRuntimeContext runtimeContext;
    private final RuntimeContext generalRuntimeContext;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Meter> meters = new ConcurrentHashMap<>();

    public FlinkMetricContext(StreamingRuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        this.generalRuntimeContext = runtimeContext;
        this.metricGroup = runtimeContext != null ? runtimeContext.getMetricGroup() : null;
    }

    public FlinkMetricContext(RuntimeContext runtimeContext, MetricGroup metricGroup) {
        this.runtimeContext =
                runtimeContext instanceof StreamingRuntimeContext
                        ? (StreamingRuntimeContext) runtimeContext
                        : null;
        this.generalRuntimeContext = runtimeContext;
        this.metricGroup = metricGroup;
    }

    public FlinkMetricContext(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.generalRuntimeContext = null;
        this.runtimeContext = null;
    }

    @Override
    public Counter counter(String name) {
        Counter existingCounter = counters.get(name);
        if (existingCounter != null) {
            return existingCounter;
        }

        org.apache.flink.metrics.Counter flinkCounter = metricGroup.counter(name);

        if (isKeyMetric(name) && generalRuntimeContext != null) {
            try {
                Counter counter =
                        new FlinkAccumulatorCounter(name, flinkCounter, generalRuntimeContext);
                counters.put(name, counter);
                return counter;
            } catch (Exception e) {
                log.warn(
                        "Failed to create accumulator for: {}, falling back to simple counter",
                        name);
            }
        }

        Counter counter = new FlinkCounter(name, flinkCounter);
        counters.put(name, counter);
        return counter;
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        return null;
    }

    @Override
    public Meter meter(String name) {
        Meter existingMeter = meters.get(name);
        if (existingMeter != null) {
            return existingMeter;
        }

        org.apache.flink.metrics.Meter flinkMeter =
                metricGroup.meter(name, new org.apache.flink.metrics.MeterView(60));
        Meter meter = new FlinkMeter(name, flinkMeter);
        meters.put(name, meter);
        return meter;
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        return null;
    }

    private boolean isKeyMetric(String name) {
        return name.equals(MetricNames.SOURCE_RECEIVED_COUNT)
                || name.equals(MetricNames.SOURCE_RECEIVED_BYTES)
                || name.equals(MetricNames.SINK_WRITE_COUNT)
                || name.equals(MetricNames.SINK_WRITE_BYTES);
    }
}

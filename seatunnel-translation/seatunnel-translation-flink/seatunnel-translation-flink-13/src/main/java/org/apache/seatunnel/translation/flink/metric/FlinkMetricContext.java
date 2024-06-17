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
import org.apache.seatunnel.api.common.metrics.Metric;
import org.apache.seatunnel.api.common.metrics.MetricsContext;

import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlinkMetricContext implements MetricsContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkMetricContext.class);

    private final Map<String, Metric> metrics = new ConcurrentHashMap<>();

    private MetricGroup metricGroup;

    private StreamingRuntimeContext runtimeContext;

    public FlinkMetricContext(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public FlinkMetricContext(StreamingRuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public Counter counter(String name) {
        if (metrics.containsKey(name)) {
            return (Counter) metrics.get(name);
        }
        Counter counter =
                runtimeContext == null
                        ? new FlinkGroupCounter(name, metricGroup.counter(name))
                        : new FlinkCounter(name, runtimeContext.getLongCounter(name));
        return this.counter(name, counter);
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        this.addMetric(name, counter);
        return counter;
    }

    @Override
    public Meter meter(String name) {
        if (metrics.containsKey(name)) {
            return (Meter) metrics.get(name);
        }

        // Why use reflection to obtain metrics group?
        // Because the value types returned by flink 1.13 and 1.14 runtimeContext.getMetricGroup()
        // are inconsistent
        org.apache.flink.metrics.Meter meter;
        if (runtimeContext == null) {
            meter = metricGroup.meter(name, new MeterView(5));
        } else {
            try {
                Field field = AbstractRuntimeUDFContext.class.getDeclaredField("metrics");
                field.setAccessible(true);
                MetricGroup mg = (MetricGroup) field.get(runtimeContext);
                meter = mg.meter(name, new MeterView(5));
            } catch (Exception e) {
                throw new IllegalStateException("Initial meter failed", e);
            }
        }
        return this.meter(name, new FlinkMeter(name, meter));
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        this.addMetric(name, meter);
        return meter;
    }

    protected void addMetric(String name, Metric metric) {
        if (metric == null) {
            LOGGER.warn("Ignoring attempted add of a metric due to being null for name {}.", name);
            return;
        }
        synchronized (this) {
            Metric prior = this.metrics.put(name, metric);
            if (prior != null) {
                this.metrics.put(name, prior);
                LOGGER.warn(
                        "Name collision: MetricsContext already contains a Metric with the name '"
                                + name
                                + "'. Metric will not be reported.");
            }
        }
    }
}

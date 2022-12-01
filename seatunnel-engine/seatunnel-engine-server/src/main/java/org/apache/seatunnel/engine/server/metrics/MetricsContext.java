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

import org.apache.seatunnel.api.common.metrics.Metric;
import org.apache.seatunnel.api.common.metrics.Unit;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;

public class MetricsContext implements DynamicMetricsProvider {

    private static final BiFunction<String, Unit, AbstractMetric> CREATE_SINGLE_WRITER_METRIC = SingleWriterMetric::new;
    private static final BiFunction<String, Unit, AbstractMetric> CREATE_THREAD_SAFE_METRICS = ThreadSafeMetric::new;

    private static final BiFunction<String, Unit, AbstractMetric> CREATE_SINGLE_WRITER_QPS_METRIC = SingleWriterQPSMetric::new;

    private volatile Map<String, AbstractMetric> metrics;

    Metric metric(String name, Unit unit) {
        return metric(name, unit, CREATE_SINGLE_WRITER_METRIC);
    }

    Metric qpsMetric(String name, Unit unit) {
        return metric(name, unit, CREATE_SINGLE_WRITER_QPS_METRIC);
    }

    Metric threadSafeMetric(String name, Unit unit) {
        return metric(name, unit, CREATE_THREAD_SAFE_METRICS);
    }

    private Metric metric(String name, Unit unit, BiFunction<String, Unit, AbstractMetric> metricSupplier) {
        if (metrics == null) { //first metric being stored
            metrics = new ConcurrentHashMap<>();
        }

        AbstractMetric metric = metrics.get(name);
        if (metric != null) {
            return metric;
        }

        metric = metricSupplier.apply(name, unit);
        metrics.put(name, metric);

        return metric;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor tagger, MetricsCollectionContext context) {
        if (metrics != null) {
            metrics.forEach((name, metric) -> {
                if (metric.get() instanceof Long) {
                    context.collect(tagger.copy(), name, ProbeLevel.INFO, toProbeUnit(metric.unit()),
                        (Long) metric.get());
                } else if (metric.get() instanceof Double) {
                    context.collect(tagger.copy(), name, ProbeLevel.INFO, toProbeUnit(metric.unit()),
                        (Double) metric.get());
                } else {
                    throw new SeaTunnelException("The value of Metric does not support " + metric.get().getClass().getSimpleName() + " data type");
                }
            });
        }
    }

    private ProbeUnit toProbeUnit(Unit unit) {
        return ProbeUnit.valueOf(unit.name());
    }

    private abstract static class AbstractMetric implements Metric {

        private final String name;
        private final Unit unit;

        AbstractMetric(String name, Unit unit) {
            this.name = name;
            this.unit = unit;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return unit;
        }

        protected abstract Object get();

    }

    private static final class SingleWriterQPSMetric extends AbstractMetric {

        private static final AtomicLongFieldUpdater<SingleWriterQPSMetric> VOLATILE_VALUE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(SingleWriterQPSMetric.class, "value");

        private volatile long value;
        private volatile long timestamp;

        SingleWriterQPSMetric(String name, Unit unit) {
            super(name, unit);
        }

        @Override
        public void set(long newValue) {
            checkAndSetStartTime();
            VOLATILE_VALUE_UPDATER.lazySet(this, newValue);
        }

        @Override
        public void increment() {
            checkAndSetStartTime();
            VOLATILE_VALUE_UPDATER.lazySet(this, value + 1);
        }

        @Override
        public void increment(long increment) {
            checkAndSetStartTime();
            VOLATILE_VALUE_UPDATER.lazySet(this, value + increment);
        }

        @Override
        public void decrement() {
            checkAndSetStartTime();
            VOLATILE_VALUE_UPDATER.lazySet(this, value - 1);
        }

        @Override
        public void decrement(long decrement) {
            checkAndSetStartTime();
            VOLATILE_VALUE_UPDATER.lazySet(this, value - decrement);
        }

        @SuppressWarnings("checkstyle:MagicNumber")
        @Override
        protected Object get() {
            long cost = System.currentTimeMillis() - timestamp;
            return (double) value * 1000 / cost;
        }

        private void checkAndSetStartTime(){
            if (timestamp == 0){
                timestamp = System.currentTimeMillis();
            }
        }
    }

    private static final class SingleWriterMetric extends AbstractMetric {

        private static final AtomicLongFieldUpdater<SingleWriterMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(SingleWriterMetric.class, "value");

        private volatile long value;

        SingleWriterMetric(String name, Unit unit) {
            super(name, unit);
        }

        @Override
        public void set(long newValue) {
            VOLATILE_VALUE_UPDATER.lazySet(this, newValue);
        }

        @Override
        public void increment() {
            VOLATILE_VALUE_UPDATER.lazySet(this, value + 1);
        }

        @Override
        public void increment(long increment) {
            VOLATILE_VALUE_UPDATER.lazySet(this, value + increment);
        }

        @Override
        public void decrement() {
            VOLATILE_VALUE_UPDATER.lazySet(this, value - 1);
        }

        @Override
        public void decrement(long decrement) {
            VOLATILE_VALUE_UPDATER.lazySet(this, value - decrement);
        }

        @Override
        protected Object get() {
            return value;
        }
    }

    private static final class ThreadSafeMetric extends AbstractMetric {

        private static final AtomicLongFieldUpdater<ThreadSafeMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(ThreadSafeMetric.class, "value");

        private volatile long value;

        ThreadSafeMetric(String name, Unit unit) {
            super(name, unit);
        }

        @Override
        public void increment() {
            VOLATILE_VALUE_UPDATER.incrementAndGet(this);
        }

        @Override
        public void increment(long amount) {
            VOLATILE_VALUE_UPDATER.addAndGet(this, amount);
        }

        @Override
        public void decrement() {
            VOLATILE_VALUE_UPDATER.decrementAndGet(this);
        }

        @Override
        public void decrement(long amount) {
            VOLATILE_VALUE_UPDATER.addAndGet(this, -amount);
        }

        @Override
        public void set(long newValue) {
            VOLATILE_VALUE_UPDATER.set(this, newValue);
        }

        @Override
        protected Object get() {
            return VOLATILE_VALUE_UPDATER.get(this);
        }
    }

}

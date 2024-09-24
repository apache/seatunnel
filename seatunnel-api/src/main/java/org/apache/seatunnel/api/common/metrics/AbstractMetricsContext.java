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

package org.apache.seatunnel.api.common.metrics;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractMetricsContext implements MetricsContext, Serializable {

    private static final long serialVersionUID = 1L;

    @Getter protected final Map<String, Metric> metrics = new ConcurrentHashMap<>();

    @Override
    public Counter counter(String name) {
        if (metrics.containsKey(name)) {
            return (Counter) metrics.get(name);
        }
        return this.counter(name, new ThreadSafeCounter(name));
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
        return this.meter(name, new ThreadSafeQPSMeter(name));
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        this.addMetric(name, meter);
        return meter;
    }

    protected void addMetric(String name, Metric metric) {
        if (metric == null) {
            log.warn("Ignoring attempted add of a metric due to being null for name {}.", name);
        } else {
            synchronized (this) {
                Metric prior = this.metrics.put(name, metric);
                if (prior != null) {
                    this.metrics.put(name, prior);
                    log.warn(
                            "Name collision: MetricsContext already contains a Metric with the name '"
                                    + name
                                    + "'. Metric will not be reported.");
                }
            }
        }
    }

    @Override
    public String toString() {
        return "AbstractMetricsContext{" + "metrics=" + metrics + '}';
    }
}

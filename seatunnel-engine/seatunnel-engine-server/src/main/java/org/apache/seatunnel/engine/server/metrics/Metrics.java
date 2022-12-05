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

public final class Metrics {

    private Metrics() {
    }

    public static Metric metric(String name) {
        return MetricsImpl.metric(name, Unit.COUNT);
    }

    /**
     * Same as {@link #metric(String)}, but allows us to also specify the
     * measurement {@link Unit} of the metric.
     */
    public static Metric metric(String name, Unit unit) {
        return MetricsImpl.metric(name, unit);
    }

    public static Metric qpsMetric(String name, Unit unit) {
        return MetricsImpl.qpsMetric(name, unit);
    }

    public static Metric threadSafeMetric(String name) {
        return MetricsImpl.threadSafeMetric(name, Unit.COUNT);
    }

    /**
     * Same as {@link #threadSafeMetric(String)}, but allows us to also
     * specify the measurement {@link Unit} of the metric.
     */
    public static Metric threadSafeMetric(String name, Unit unit) {
        return MetricsImpl.threadSafeMetric(name, unit);
    }
}

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

package org.apache.seatunnel.engine.server.autoscale;

import java.io.Serializable;
import java.util.Objects;

public final class MetricValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final MetricStatus status;
    private final Double value;

    private MetricValue(MetricStatus status, Double value) {
        this.status = Objects.requireNonNull(status, "status");
        this.value = value;
    }

    public static MetricValue valid(double value) {
        if (!Double.isFinite(value) || value < 0.0d || value > 1.0d) {
            return invalid();
        }
        return new MetricValue(MetricStatus.VALID, value);
    }

    public static MetricValue missing() {
        return new MetricValue(MetricStatus.MISSING, null);
    }

    public static MetricValue stale() {
        return new MetricValue(MetricStatus.STALE, null);
    }

    public static MetricValue future() {
        return new MetricValue(MetricStatus.FUTURE, null);
    }

    public static MetricValue invalid() {
        return new MetricValue(MetricStatus.INVALID, null);
    }

    public static MetricValue unknown() {
        return new MetricValue(MetricStatus.UNKNOWN, null);
    }

    public MetricStatus getStatus() {
        return status;
    }

    public Double getValue() {
        return value;
    }

    public boolean isValid() {
        return status == MetricStatus.VALID;
    }

    public boolean isGreaterThanOrEqualTo(double threshold) {
        return isValid() && value >= threshold;
    }

    public boolean isLessThan(double threshold) {
        return isValid() && value < threshold;
    }
}

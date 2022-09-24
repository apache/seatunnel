package org.apache.seatunnel.metrics.core;

/** A Gauge is a {@link Metric} that calculates a specific value at a point in time. */
public interface Gauge<T> extends Metric{
    T getValue();

    default MetricType getMetricType() {
        return MetricType.GAUGE;
    }
}

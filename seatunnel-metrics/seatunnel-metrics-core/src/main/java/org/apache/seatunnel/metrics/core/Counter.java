package org.apache.seatunnel.metrics.core;

/** A Counter is a {@link Metric} that measures a count. */
public interface Counter extends Metric{
    void inc();

    void inc(long var1);

    void dec();

    void dec(long var1);

    long getCount();

    default MetricType getMetricType() {
        return MetricType.COUNTER;
    }
}

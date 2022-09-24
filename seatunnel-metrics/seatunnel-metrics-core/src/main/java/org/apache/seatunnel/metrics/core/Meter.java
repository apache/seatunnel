package org.apache.seatunnel.metrics.core;

/** Metric for measuring throughput. */
public interface Meter extends Metric{

    double getRate();

    long getCount();

    default MetricType getMetricType() {
        return MetricType.METER;
    }
}

package org.apache.seatunnel.metrics.core.reporter;

import org.apache.seatunnel.metrics.core.*;

import java.util.Map;

/** Reporters are used to export seatunnel {@link Metric Metrics} to an external backend. */
public interface MetricReporter {
    MetricReporter open();

    void close();

    void report(Map<Gauge, MetricInfo> gauges,
                Map<Counter, MetricInfo> counters,
                Map<Histogram, MetricInfo> histograms,
                Map<Meter, MetricInfo> meters);
}

package org.apache.seatunnel.metrics.core.reporter;

import org.apache.seatunnel.metrics.core.Counter;
import org.apache.seatunnel.metrics.core.Gauge;
import org.apache.seatunnel.metrics.core.Histogram;
import org.apache.seatunnel.metrics.core.Meter;
import org.apache.seatunnel.metrics.core.Metric;
import org.apache.seatunnel.metrics.core.MetricInfo;

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

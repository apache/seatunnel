package org.apache.seatunnel.metrics.console;

import org.apache.seatunnel.metrics.core.Counter;
import org.apache.seatunnel.metrics.core.Gauge;
import org.apache.seatunnel.metrics.core.Histogram;
import org.apache.seatunnel.metrics.core.Meter;
import org.apache.seatunnel.metrics.core.MetricConfig;
import org.apache.seatunnel.metrics.core.MetricInfo;
import org.apache.seatunnel.metrics.core.reporter.MetricReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A reporter which outputs measurements to log
 */
public class ConsoleLogReporter implements MetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleLogReporter.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final int DEFAULT_SIZE = 16384;
    private int previousSize = DEFAULT_SIZE;

    @Override
    public void open(MetricConfig config) {
        LOG.info("reporter open");
    }

    @Override
    public void close() {
        LOG.info("reporter close");
    }

    @Override
    public void report(Map<Gauge, MetricInfo> gauges,
                       Map<Counter, MetricInfo> counters,
                       Map<Histogram, MetricInfo> histograms,
                       Map<Meter, MetricInfo> meters) {
        final double multiple = 1.1;
        StringBuilder builder = new StringBuilder((int) (previousSize * multiple));

        builder.append(LINE_SEPARATOR)
            .append(
                "=========================== Starting metrics report ===========================")
            .append(LINE_SEPARATOR);

        builder.append(LINE_SEPARATOR)
            .append(
                "-- Counters -------------------------------------------------------------------")
            .append(LINE_SEPARATOR);

        for (Map.Entry<Counter, MetricInfo> metric : counters.entrySet()) {
            builder.append(metric.getValue().toString())
                .append(metric.getKey().getMetricType().toString())
                .append(": ")
                .append(metric.getKey().getCount())
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR);

        }

        builder.append(LINE_SEPARATOR)
            .append(
                "-- Gauges -------------------------------------------------------------------")
            .append(LINE_SEPARATOR);

        for (Map.Entry<Gauge, MetricInfo> metric : gauges.entrySet()) {
            builder.append(metric.getValue().toString())
                .append(metric.getKey().getMetricType().toString())
                .append(": ")
                .append(metric.getKey().getValue())
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR);

        }

        builder.append(LINE_SEPARATOR)
            .append(
                "-- Meters -------------------------------------------------------------------")
            .append(LINE_SEPARATOR);

        for (Map.Entry<Meter, MetricInfo> metric : meters.entrySet()) {
            builder.append(metric.getValue().toString())
                .append(metric.getKey().getMetricType().toString())
                .append(": ")
                .append(metric.getKey().getRate())
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR);

        }

        builder.append(LINE_SEPARATOR)
            .append(
                "-- Histograms -------------------------------------------------------------------")
            .append(LINE_SEPARATOR);

        for (Map.Entry<Histogram, MetricInfo> metric : histograms.entrySet()) {
            builder.append(metric.getValue().toString())
                .append(metric.getKey().getMetricType().toString())
                .append(LINE_SEPARATOR)
                .append(metric.getKey().toString())
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR);

        }

        LOG.info(builder.toString());

        previousSize = builder.length();

    }

}

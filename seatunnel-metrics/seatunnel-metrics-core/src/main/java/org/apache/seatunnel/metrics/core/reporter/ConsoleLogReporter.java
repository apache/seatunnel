package org.apache.seatunnel.metrics.core.reporter;

import org.apache.seatunnel.metrics.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**  A reporter which outputs measurements to log */
public class ConsoleLogReporter implements MetricReporter{

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleLogReporter.class);
    private static final String lineSeparator = System.lineSeparator();
    private int previousSize = 16384;

    @Override
    public ConsoleLogReporter open() {
        LOG.info("reporter open");
        return new ConsoleLogReporter();
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
        StringBuilder builder = new StringBuilder((int) (previousSize * 1.1));

        builder.append(lineSeparator)
                .append(
                        "=========================== Starting metrics report ===========================")
                .append(lineSeparator);

        builder.append(lineSeparator)
                .append(
                        "-- Counters -------------------------------------------------------------------")
                .append(lineSeparator);

        for (Map.Entry<Counter, MetricInfo> metric : counters.entrySet()) {
            builder.append(metric.getValue().toString())
                    .append(metric.getKey().getMetricType().toString())
                    .append(": ")
                    .append(metric.getKey().getCount())
                    .append(lineSeparator)
                    .append(lineSeparator);

        }

        builder.append(lineSeparator)
                .append(
                        "-- Gauges -------------------------------------------------------------------")
                .append(lineSeparator);

        for (Map.Entry<Gauge, MetricInfo> metric : gauges.entrySet()) {
            builder.append(metric.getValue().toString())
                    .append(metric.getKey().getMetricType().toString())
                    .append(": ")
                    .append(metric.getKey().getValue())
                    .append(lineSeparator)
                    .append(lineSeparator);

        }

        builder.append(lineSeparator)
                .append(
                        "-- Meters -------------------------------------------------------------------")
                .append(lineSeparator);

        for (Map.Entry<Meter, MetricInfo> metric : meters.entrySet()) {
            builder.append(metric.getValue().toString())
                    .append(metric.getKey().getMetricType().toString())
                    .append(": ")
                    .append(metric.getKey().getRate())
                    .append(lineSeparator)
                    .append(lineSeparator);

        }

        builder.append(lineSeparator)
                .append(
                        "-- Histograms -------------------------------------------------------------------")
                .append(lineSeparator);

        for (Map.Entry<Histogram, MetricInfo> metric : histograms.entrySet()) {
            builder.append(metric.getValue().toString())
                    .append(metric.getKey().getMetricType().toString())
                    .append(lineSeparator)
                    .append(metric.getKey().toString())
                    .append(lineSeparator)
                    .append(lineSeparator);

        }


        LOG.info(builder.toString());

        previousSize = builder.length();

    }


}

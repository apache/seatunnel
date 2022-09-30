package org.apache.seatunnel.metrics.flink;

import org.apache.seatunnel.metrics.core.Counter;
import org.apache.seatunnel.metrics.core.Gauge;
import org.apache.seatunnel.metrics.core.Histogram;
import org.apache.seatunnel.metrics.core.Meter;
import org.apache.seatunnel.metrics.core.MetricInfo;
import org.apache.seatunnel.metrics.core.SimpleCounter;
import org.apache.seatunnel.metrics.core.SimpleGauge;
import org.apache.seatunnel.metrics.core.SimpleHistogram;
import org.apache.seatunnel.metrics.core.SimpleMeter;
import org.apache.seatunnel.metrics.core.reporter.MetricReporter;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * exports Flink metrics to Seatunnel
 */
public class SeatunnelMetricReporter extends AbstractSeatunnelReporter implements Scheduled {
    private final Logger log = LoggerFactory.getLogger(SeatunnelMetricReporter.class);
    private MetricReporter reporter;
    private String host;
    private int port;
    private String jobName;
    private String className;
    private static final int DEFAULT_PORT = 9091;

    @Override
    public void open(MetricConfig metricConfig) {
        MetricConfig config = metricConfig;
        config.isEmpty();
        host = config.getString("host", "localhost");
        port = config.getInteger("port", DEFAULT_PORT);
        jobName = config.getString("jobName", "flinkJob");
        className = config.getString("reporterName", "org.apache.seatunnel.metrics.console.ConsoleLogReporter");
    }

    @Override
    public void close() {
        log.info("StreamMetricReporter close");
    }

    @Override
    public void report() {
        log.info("reporter report");
        HashMap<Counter, MetricInfo> countersIndex = new HashMap<>();
        HashMap<Gauge, MetricInfo> gaugesIndex = new HashMap<>();
        HashMap<Histogram, MetricInfo> histogramsIndex = new HashMap<>();
        HashMap<Meter, MetricInfo> metersIndex = new HashMap<>();

        HashSet<String> name = new HashSet<>();

        //Convert flink metrics to seatunnel
        for (Map.Entry<org.apache.flink.metrics.Counter, MetricInfo> metric : counters.entrySet()) {
            //Skip processing on a repeat
            if (name.contains(metric.getValue().getMetricName())) {
                continue;
            }
            name.add(metric.getValue().getMetricName());
            countersIndex.put(new SimpleCounter(metric.getKey().getCount()), metric.getValue());
        }

        name.clear();
        for (Map.Entry<org.apache.flink.metrics.Gauge<?>, MetricInfo> metric : gauges.entrySet()) {
            //Skip processing on a repeat
            if (name.contains(metric.getValue().getMetricName())) {
                continue;
            }
            name.add(metric.getValue().getMetricName());
            Object num = metric.getKey().getValue();
            if (num instanceof Number) {
                gaugesIndex.put(new SimpleGauge((Number) num), metric.getValue());
            }
        }

        name.clear();
        for (Map.Entry<org.apache.flink.metrics.Meter, MetricInfo> metric : meters.entrySet()) {
            //Skip processing on a repeat
            if (name.contains(metric.getValue().getMetricName())) {
                continue;
            }
            name.add(metric.getValue().getMetricName());
            metersIndex.put(new SimpleMeter(metric.getKey().getRate(), metric.getKey().getCount()), metric.getValue());

        }
        final double quantile05 = 0.5;
        final double quantile75 = 0.75;
        final double quantile95 = 0.95;
        //todo histogram
        for (Map.Entry<org.apache.flink.metrics.Histogram, MetricInfo> metric : histograms.entrySet()) {
            org.apache.flink.metrics.Histogram key = metric.getKey();
            HashMap<Double, Double> quantile = new HashMap<>();
            quantile.put(quantile05, key.getStatistics().getQuantile(quantile05));
            quantile.put(quantile75, key.getStatistics().getQuantile(quantile75));
            quantile.put(quantile95, key.getStatistics().getQuantile(quantile95));
            histogramsIndex.put(new SimpleHistogram(key.getCount(), key.getStatistics().getMin(), key.getStatistics().getMax(), key.getStatistics().getStdDev(), key.getStatistics().getMean(), quantile), metric.getValue());
        }
        //todo handle user config
        try {
            //ClassLoader classLoader = PrometheusPushGatewayReporter.class.getClassLoader();
            Class<?> aClass = Class.forName(className);
            reporter = (MetricReporter) aClass.newInstance();
            org.apache.seatunnel.metrics.core.MetricConfig config = new org.apache.seatunnel.metrics.core.MetricConfig();
            config.setJobName(jobName);
            config.setHost(host);
            config.setPort(port);
            reporter.open(config);
            reporter.report(gaugesIndex, countersIndex, histogramsIndex, metersIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}


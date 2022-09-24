package org.apache.seatunnel.metrics.core.reporter;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.apache.seatunnel.metrics.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * A reporter which outputs measurements to PrometheusPushGateway
 */
public class PrometheusPushGatewayReporter implements MetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewayReporter.class);
    final URL hostUrl;
    private final PushGateway pushGateway;
    private final String jobName;

    public PrometheusPushGatewayReporter(String jobName, String host, int port) {
        String url = "";
        if (isNullOrWhitespaceOnly(host) || port < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        } else {
            url = "http://" + host + ":" + port;
        }

        this.jobName = jobName;
        try {
            this.hostUrl = new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        this.pushGateway = new PushGateway(hostUrl);
    }

    @Override
    public PrometheusPushGatewayReporter open() {
        //todo Handle user config
        return new PrometheusPushGatewayReporter("flink_prometheus_job", "localhost", 9091);
    }

    @Override
    public void close() {

    }

    @Override
    public void report(Map<Gauge, MetricInfo> gauges,
                       Map<Counter, MetricInfo> counters,
                       Map<Histogram, MetricInfo> histograms,
                       Map<Meter, MetricInfo> meters) {
        Collector collector;
        CollectorRegistry registry = new CollectorRegistry();
        for (Map.Entry<Counter, MetricInfo> metric : counters.entrySet()) {
            MetricInfo metricInfo = metric.getValue();
            collector = createCollector(metric.getKey(), metricInfo.getMetricName(), metricInfo.getHelpString(), metricInfo.getDimensionKeys(), metricInfo.getDimensionValues());
            try {
                collector.register(registry);
            } catch (Exception e) {
                LOG.warn("There was a problem registering metric {}.", metric.getValue().toString(), e);
            }
            addMetric(metric.getKey(), metricInfo.getDimensionValues(), collector);
        }

        for (Map.Entry<Gauge, MetricInfo> metric : gauges.entrySet()) {
            MetricInfo metricInfo = metric.getValue();
            collector = createCollector(metric.getKey(), metricInfo.getMetricName(), metricInfo.getHelpString(), metricInfo.getDimensionKeys(), metricInfo.getDimensionValues());
            try {
                collector.register(registry);
            } catch (Exception e) {
                LOG.warn("There was a problem registering metric {}.", metric.getValue().toString(), e);
            }
            addMetric(metric.getKey(), metricInfo.getDimensionValues(), collector);
        }

        //todo:add histogram
//        for (Map.Entry<Histogram, MetricInfo> metric : histograms.entrySet()) {
//            MetricInfo metricInfo = metric.getValue();
//            collector = createCollector(metric.getKey(),metricInfo.getMetricName(),metricInfo.getHelpString(),metricInfo.getDimensionKeys(),metricInfo.getDimensionValues());
//            try {
//                collector.register(registry);
//            } catch (Exception e) {
//                LOG.warn("There was a problem registering metric {}.", metric.getValue().toString(), e);
//            }
//            addMetric(metric.getKey(),metricInfo.getDimensionValues() , collector);
//        }

        for (Map.Entry<Meter, MetricInfo> metric : meters.entrySet()) {
            MetricInfo metricInfo = metric.getValue();
            collector = createCollector(metric.getKey(), metricInfo.getMetricName(), metricInfo.getHelpString(), metricInfo.getDimensionKeys(), metricInfo.getDimensionValues());
            try {
                collector.register(registry);
            } catch (Exception e) {
                LOG.warn("There was a problem registering metric {}.", metric.getValue().toString(), e);
            }
            addMetric(metric.getKey(), metricInfo.getDimensionValues(), collector);
        }


        try {
            pushGateway.pushAdd(registry, jobName);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to push metrics to PushGateway with jobName {}.",
                    jobName,
                    e);
        }
    }

    private Collector createCollector(Metric metric,
                                      String metricName,
                                      String helpString,
                                      List<String> dimensionKeys,
                                      List<String> dimensionValues) {
        Collector collector;
        switch (metric.getMetricType()) {
            case GAUGE:
            case COUNTER:
            case METER:
                collector =
                        io.prometheus.client.Gauge.build()
                                .name(metricName)
                                .help(helpString)
                                .labelNames(toArray(dimensionKeys))
                                .create();
                break;
            case HISTOGRAM:
                collector =
                        io.prometheus.client.Histogram.build()
                                .name(metricName)
                                .labelNames(toArray(dimensionKeys))
                                .create();
                break;
            default:
                LOG.warn(
                        "Cannot create collector for unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
                        metric.getClass().getName());
                collector = null;
        }

        return collector;
    }

    private void addMetric(Metric metric, List<String> dimensionValues, Collector collector) {
        switch (metric.getMetricType()) {
            case GAUGE:
                ((io.prometheus.client.Gauge) collector)
                        .setChild(gaugeFrom((Gauge<?>) metric), toArray(dimensionValues));
                break;
            case COUNTER:
                ((io.prometheus.client.Gauge) collector)
                        .setChild(gaugeFrom((Counter) metric), toArray(dimensionValues));
                break;
            case METER:
                ((io.prometheus.client.Gauge) collector)
                        .setChild(gaugeFrom((Meter) metric), toArray(dimensionValues));
                break;
            case HISTOGRAM:
                // todo
                LOG.error("to do");
                break;
            default:
                LOG.warn(
                        "Cannot add unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
                        metric.getClass().getName());
        }
    }

    private static io.prometheus.client.Gauge.Child gaugeFrom(Gauge<?> gauge) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                final Object value = gauge.getValue();
                if (value == null) {
                    LOG.debug("Gauge {} is null-valued, defaulting to 0.", gauge);
                    return 0;
                }
                if (value instanceof Double) {
                    return (double) value;
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                if (value instanceof Boolean) {
                    return ((Boolean) value) ? 1 : 0;
                }
                LOG.debug(
                        "Invalid type for Gauge {}: {}, only number types and booleans are supported by this reporter.",
                        gauge,
                        value.getClass().getName());
                return 0;
            }
        };
    }

    private static io.prometheus.client.Gauge.Child gaugeFrom(Counter counter) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                return (double) counter.getCount();
            }
        };
    }

    private static io.prometheus.client.Gauge.Child gaugeFrom(Meter meter) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                return meter.getRate();
            }
        };
    }

    private static String[] toArray(List<String> list) {
        return list.toArray(new String[list.size()]);
    }

    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str == null || str.length() == 0) {
            return true;
        }

        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}

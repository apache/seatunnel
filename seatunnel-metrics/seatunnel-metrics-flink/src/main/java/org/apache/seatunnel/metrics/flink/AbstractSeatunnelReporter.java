package org.apache.seatunnel.metrics.flink;

import org.apache.seatunnel.metrics.core.MetricInfo;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** base seatunnel reporter for flink metrics. */
public abstract class AbstractSeatunnelReporter implements MetricReporter {

    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
        public String filterCharacters(String input) {
            return AbstractSeatunnelReporter.replaceInvalidChars(input);
        }
    };

    private CharacterFilter labelValueCharactersFilter = CHARACTER_FILTER;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final Map<Gauge<?>, MetricInfo> gauges = new HashMap<>();
    protected final Map<Counter, MetricInfo> counters = new HashMap<>();
    protected final Map<Histogram, MetricInfo> histograms = new HashMap<>();
    protected final Map<Meter, MetricInfo> meters = new HashMap<>();

    public AbstractSeatunnelReporter() {
    }

    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String scopedMetricName = getScopedName(metricName, group);
        String helpString = metricName + " (scope: " + getLogicalScope(group) + ")";
        List<String> dimensionKeys = new LinkedList<>();
        List<String> dimensionValues = new LinkedList<>();
        for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
            final String key = dimension.getKey();
            dimensionKeys.add(
                    CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)));
            dimensionValues.add(labelValueCharactersFilter.filterCharacters(dimension.getValue()));
        }
        String metricNameFin = scopedMetricName;
        if (dimensionKeys.size() > 0 && dimensionKeys.get(dimensionKeys.size() - 1).equals("subtask_index")) {
            metricNameFin = scopedMetricName + "_" + dimensionValues.get(dimensionValues.size() - 1);
        }
        MetricInfo metricInfo = new MetricInfo(metricNameFin, helpString, dimensionKeys, dimensionValues);
        synchronized (this) {
            if (metric instanceof Counter) {
                this.counters.put((Counter) metric, metricInfo);
            } else if (metric instanceof Gauge) {
                this.gauges.put((Gauge) metric, metricInfo);
            } else if (metric instanceof Histogram) {
                this.histograms.put((Histogram) metric, metricInfo);
            } else if (metric instanceof Meter) {
                this.meters.put((Meter) metric, metricInfo);
            } else {
                this.log.warn("Cannot add unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
            }

        }
    }

    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            if (metric instanceof Counter) {
                this.counters.remove(metric);
            } else if (metric instanceof Gauge) {
                this.gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                this.histograms.remove(metric);
            } else if (metric instanceof Meter) {
                this.meters.remove(metric);
            } else {
                this.log.warn("Cannot remove unknown metric type {}. This indicates that the reporter does not support this metric type.", metric.getClass().getName());
            }

        }
    }

    private static String getScopedName(String metricName, MetricGroup group) {
        return "seatunnel_" + getLogicalScope(group) + '_' + CHARACTER_FILTER.filterCharacters(metricName);
    }

    private static String getLogicalScope(MetricGroup group) {
        return ((FrontMetricGroup<AbstractMetricGroup<?>>) group)
                .getLogicalScope(CHARACTER_FILTER, '_');
    }

    static String replaceInvalidChars(String input) {
        return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }
}

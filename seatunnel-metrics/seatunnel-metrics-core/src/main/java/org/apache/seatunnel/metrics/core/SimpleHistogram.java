package org.apache.seatunnel.metrics.core;

import java.util.Map;

public class SimpleHistogram implements Histogram{
    private long count;
    private double min;
    private double max;
    private double stdDev;
    private double mean;
    private Map<Double, Double> quantile;

    public SimpleHistogram(long count, double min, double max, double stdDev, double mean, Map<Double, Double> quantile) {
        this.count = count;
        this.min = min;
        this.max = max;
        this.stdDev = stdDev;
        this.mean = mean;
        this.quantile = quantile;
    }

    @Override
    public long getCount() {
        return this.count;
    }

    @Override
    public double getMin() {
        return this.min;
    }

    @Override
    public double getMax() {
        return this.max;
    }

    @Override
    public double getStdDev() {
        return this.stdDev;
    }

    @Override
    public double getMean() {
        return this.mean;
    }

    @Override
    public Map<Double, Double> getQuantile() {
        return this.quantile;
    }

    @Override
    public String toString() {
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        builder.append("count: ")
                .append(count)
                .append(lineSeparator)
                .append("min: ")
                .append(min)
                .append(lineSeparator)
                .append("max: ")
                .append(max)
                .append(lineSeparator)
                .append("stdDev: ")
                .append(stdDev)
                .append(lineSeparator)
                .append("mean: ")
                .append(mean)
                .append(lineSeparator);
        return builder.toString();
    }
}

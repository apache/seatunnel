package org.apache.seatunnel.metrics.core;

import java.util.List;

/** Stores all child-properties of a metric. */
public class MetricInfo {
    private String metricName;
    private List<String> dimensionKeys;
    private List<String> dimensionValues;
    private String helpString;

    public String getMetricName() {
        return metricName;
    }

    public String getHelpString() {
        return helpString;
    }

    public List<String> getDimensionKeys() {
        return dimensionKeys;
    }

    public List<String> getDimensionValues() {
        return dimensionValues;
    }
    public MetricInfo(String metricName, String helpString, List<String> dimensionKeys, List<String> dimensionValues) {
        this.metricName = metricName;
        this.helpString = helpString;
        this.dimensionKeys = dimensionKeys;
        this.dimensionValues = dimensionValues;
    }

    @Override
    public String toString() {
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        builder.append("metricName: ")
                .append(this.metricName)
                .append(lineSeparator);
        builder.append("helpString: ")
                .append(this.helpString)
                .append(lineSeparator);
        for(int i=0;i<this.dimensionKeys.size();i++){
            builder.append(dimensionKeys.get(i))
                    .append(": ")
                    .append(dimensionValues.get(i))
                    .append(lineSeparator);
        }
        return builder.toString();
    }
}

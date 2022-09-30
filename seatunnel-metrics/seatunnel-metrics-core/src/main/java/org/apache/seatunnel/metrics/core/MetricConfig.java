package org.apache.seatunnel.metrics.core;

import java.util.HashMap;
import java.util.Map;

public class MetricConfig {
    private String jobName;
    private String host;
    private int port;
    private Map configs;

    public MetricConfig() {
        configs = new HashMap<String, String>();
        jobName = "config";
        host = "localhost";
        port = 0;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Map getConfigs() {
        return configs;
    }

    public void setConfigs(Map configs) {
        this.configs = configs;
    }
}

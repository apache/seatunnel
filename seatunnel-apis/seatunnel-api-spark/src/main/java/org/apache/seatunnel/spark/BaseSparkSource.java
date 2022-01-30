package org.apache.seatunnel.spark;

import org.apache.seatunnel.apis.BaseSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

public abstract class BaseSparkSource<T> implements BaseSource<SparkEnvironment> {

    protected Config config = ConfigFactory.empty();

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    public abstract T getData(SparkEnvironment env);
}

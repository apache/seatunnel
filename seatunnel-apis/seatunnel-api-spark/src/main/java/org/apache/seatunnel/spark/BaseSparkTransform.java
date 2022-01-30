package org.apache.seatunnel.spark;

import org.apache.seatunnel.apis.BaseTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class BaseSparkTransform implements BaseTransform<SparkEnvironment> {

    protected Config config = ConfigFactory.empty();

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    public abstract Dataset<Row> process(Dataset<Row> data, SparkEnvironment env);
}

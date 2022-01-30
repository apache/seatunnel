package org.apache.seatunnel.spark;

import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.common.config.CheckResult;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class BaseSparkSink<OUT> implements BaseSink<SparkEnvironment> {

    protected Config config = ConfigFactory.empty();

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    public abstract CheckResult checkConfig();

    public abstract void prepare(SparkEnvironment prepareEnv);

    public abstract OUT output(Dataset<Row> data, SparkEnvironment env);
}

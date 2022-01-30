package org.apache.seatunnel.spark.structuredstream;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import java.util.List;

public class StructuredStreamingExecution implements Execution<StructuredStreamingSource, BaseSparkTransform, StructuredStreamingSink> {

    private final SparkEnvironment sparkEnvironment;

    private Config config = ConfigFactory.empty();

    public StructuredStreamingExecution(SparkEnvironment sparkEnvironment) {
        this.sparkEnvironment = sparkEnvironment;
    }

    @Override
    public void start(List<StructuredStreamingSource> sources, List<BaseSparkTransform> transforms, List<StructuredStreamingSink> sinks) {

    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckResult.success();
    }

    @Override
    public void prepare(Void prepareEnv) {

    }
}

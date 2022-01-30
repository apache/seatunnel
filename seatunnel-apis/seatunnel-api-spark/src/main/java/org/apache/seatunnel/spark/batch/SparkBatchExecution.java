package org.apache.seatunnel.spark.batch;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.spark.BaseSparkSink;
import org.apache.seatunnel.spark.BaseSparkSource;
import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class SparkBatchExecution implements Execution<SparkBatchSource, BaseSparkTransform, SparkBatchSink> {

    public static final String SOURCE_TABLE_NAME = "source_table_name";
    public static final String RESULT_TABLE_NAME = "result_table_name";

    public static void registerTempView(String tableName, Dataset<Row> ds) {
        ds.createOrReplaceGlobalTempView(tableName);
    }

    public static void registerInputTempView(BaseSparkSource<Dataset<Row>> source, SparkEnvironment environment) {
        Config config = source.getConfig();
        if (config.hasPath(SparkBatchExecution.RESULT_TABLE_NAME)) {
            String tableName = config.getString(SparkBatchExecution.RESULT_TABLE_NAME);
            registerTempView(tableName, source.getData(environment));
        } else {
            throw new ConfigRuntimeException(
                    "Plugin[" + source.getClass().getName() + "] must be registered as dataset/table, please set \"result_table_name\" config");
        }
    }

    public static Dataset<Row> transformProcess(SparkEnvironment environment, BaseSparkTransform transform, Dataset<Row> ds) {
        Dataset<Row> fromDs;
        Config config = transform.getConfig();
        if (config.hasPath(SparkBatchExecution.SOURCE_TABLE_NAME)) {
            String sourceTableName = config.getString(SparkBatchExecution.SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }
        return transform.process(fromDs, environment);
    }

    public static void registerTransformTempView(BaseSparkTransform transform, Dataset<Row> ds) {
        Config config = transform.getConfig();
        if (config.hasPath(SparkBatchExecution.RESULT_TABLE_NAME)) {
            config.getString(SparkBatchExecution.RESULT_TABLE_NAME);
            registerTempView(RESULT_TABLE_NAME, ds);
        }
    }

    public static void sinkProcess(SparkEnvironment environment, BaseSparkSink<?> sink, Dataset<Row> ds) {
        Dataset<Row> fromDs;
        Config config = sink.getConfig();
        if (config.hasPath(SparkBatchExecution.SOURCE_TABLE_NAME)) {
            String sourceTableName = config.getString(SparkBatchExecution.RESULT_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }
        sink.output(fromDs, environment);
    }

    private final SparkEnvironment environment;

    private Config config = ConfigFactory.empty();

    public SparkBatchExecution(SparkEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public void start(List<SparkBatchSource> sources, List<BaseSparkTransform> transforms, List<SparkBatchSink> sinks) {
        sources.forEach(source -> SparkBatchExecution.registerInputTempView(source, environment));
        if (!sources.isEmpty()) {
            Dataset<Row> ds = sources.get(0).getData(environment);
            for (BaseSparkTransform transform : transforms) {
                if (ds.head().size() > 0) {
                    ds = SparkBatchExecution.transformProcess(environment, transform, ds);
                    SparkBatchExecution.registerTransformTempView(transform, ds);
                }
            }
            for (SparkBatchSink sink : sinks) {
                SparkBatchExecution.sinkProcess(environment, sink, ds);
            }
        }
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

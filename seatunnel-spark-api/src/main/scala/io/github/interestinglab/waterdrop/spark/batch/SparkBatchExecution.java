/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.interestinglab.waterdrop.spark.batch;

import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.spark.BaseSparkSink;
import io.github.interestinglab.waterdrop.spark.BaseSparkSource;
import io.github.interestinglab.waterdrop.spark.BaseSparkTransform;
import io.github.interestinglab.waterdrop.spark.SparkEnvironment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class SparkBatchExecution implements Execution<BaseSparkBatchSource, BaseSparkTransform, BaseSparkBatchSink> {
    private SparkEnvironment environment;
    private Config config = ConfigFactory.empty();



    public SparkBatchExecution(SparkEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public void start(List<BaseSparkBatchSource> sources, List<BaseSparkTransform> transforms, List<BaseSparkBatchSink> sinks) {
        sources.forEach(s -> {
            BaseSparkSource<Dataset<Row>> ds = s;
            registerInputTempView(ds, environment);
        });
        if (!sources.isEmpty()) {
            Dataset<Row> ds = sources.get(0).getData(environment);
            for (BaseSparkTransform tf : transforms) {
                if (ds.head().size() > 0) {
                    ds = SparkBatchExecution.transformProcess(environment, tf, ds);
                    SparkBatchExecution.registerTransformTempView(tf, ds);
                }
            }

            // if (ds.take(1).length > 0) {
            for (BaseSparkBatchSink sink : sinks) {
                SparkBatchExecution.sinkProcess(environment, sink, ds);
            }
            // }
        }
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Void prepareEnv) {

    }

    public static void registerTempView(String tableName, Dataset<Row> ds) {
        ds.createOrReplaceTempView(tableName);
    }

    public static void registerInputTempView(BaseSparkSource<Dataset<Row>> source, SparkEnvironment environment) {
        Config conf = source.getConfig();
        final boolean flag = conf.hasPath(SparkBatchExecution.RESULT_TABLE_NAME);

        if (flag) {
            String tableName = conf.getString(SparkBatchExecution.RESULT_TABLE_NAME);
            registerTempView(tableName, source.getData(environment));
        } else {
            throw new ConfigRuntimeException(
                    "Plugin[" + source.getClass().getName() + "] must be registered as dataset/table, please set \"result_table_name\" config");
        }
    }

    public static Dataset<Row> transformProcess(SparkEnvironment environment, BaseSparkTransform transform, Dataset<Row> ds) {
        Config config = transform.getConfig();
        boolean flag = config.hasPath(SparkBatchExecution.SOURCE_TABLE_NAME);
        Dataset<Row> fromDs;
        if (flag) {
            String sourceTableName = config.getString(SparkBatchExecution.SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);

        } else {
            fromDs = ds;
        }

        return transform.process(fromDs, environment);
    }


    public static void registerTransformTempView(BaseSparkTransform plugin, Dataset<Row> ds) {
        Config config = plugin.getConfig();
        if (config.hasPath(SparkBatchExecution.RESULT_TABLE_NAME)) {
            String tableName = config.getString(SparkBatchExecution.RESULT_TABLE_NAME);
            registerTempView(tableName, ds);
        }
    }

    public static void sinkProcess(SparkEnvironment environment, BaseSparkSink sink, Dataset<Row> ds) {
        Config config = sink.getConfig();
        boolean flag = config.hasPath(SparkBatchExecution.SOURCE_TABLE_NAME);
        Dataset<Row> fromDs;
        if (flag){
            String sourceTableName = config.getString(SparkBatchExecution.SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        }else {
            fromDs = ds;
        }


        sink.output(fromDs, environment);
    }


}

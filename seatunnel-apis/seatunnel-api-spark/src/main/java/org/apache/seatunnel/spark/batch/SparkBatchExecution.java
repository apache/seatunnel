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

    private final SparkEnvironment environment;

    private Config config = ConfigFactory.empty();

    public SparkBatchExecution(SparkEnvironment environment) {
        this.environment = environment;
    }

    public static void registerTempView(String tableName, Dataset<Row> ds) {
        ds.createOrReplaceTempView(tableName);
    }

    public static void registerInputTempView(BaseSparkSource<Dataset<Row>> source, SparkEnvironment environment) {
        Config config = source.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String tableName = config.getString(RESULT_TABLE_NAME);
            registerTempView(tableName, source.getData(environment));
        } else {
            throw new ConfigRuntimeException("Plugin[" + source.getClass().getName() + "] " +
                "must be registered as dataset/table, please set \"" + RESULT_TABLE_NAME + "\" config");
        }
    }

    public static Dataset<Row> transformProcess(SparkEnvironment environment, BaseSparkTransform transform, Dataset<Row> ds) {
        Dataset<Row> fromDs;
        Config config = transform.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            String sourceTableName = config.getString(SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }
        return transform.process(fromDs, environment);
    }

    public static void registerTransformTempView(BaseSparkTransform transform, Dataset<Row> ds) {
        Config config = transform.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String resultTableName = config.getString(RESULT_TABLE_NAME);
            registerTempView(resultTableName, ds);
        }
    }

    public static void sinkProcess(SparkEnvironment environment, BaseSparkSink<?> sink, Dataset<Row> ds) {
        Dataset<Row> fromDs;
        Config config = sink.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            String sourceTableName = config.getString(SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }
        sink.output(fromDs, environment);
    }

    @Override
    public void start(List<SparkBatchSource> sources, List<BaseSparkTransform> transforms, List<SparkBatchSink> sinks) {
        sources.forEach(source -> registerInputTempView(source, environment));
        if (!sources.isEmpty()) {
            Dataset<Row> ds = sources.get(0).getData(environment);
            for (BaseSparkTransform transform : transforms) {
                if (ds.takeAsList(1).size() > 0) {
                    ds = transformProcess(environment, transform, ds);
                    registerTransformTempView(transform, ds);
                }
            }
            for (SparkBatchSink sink : sinks) {
                sinkProcess(environment, sink, ds);
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

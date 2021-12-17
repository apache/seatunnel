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

package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.common.PropertiesUtil;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

public class DorisSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private Config config;
    private String fenodes;
    private String username;
    private String password;
    private String tableName;
    private String dbName;
    private int batchSize = 5000;
    private long batchIntervalMs = 10;
    private int maxRetries = 1;
    private Properties streamLoadProp = new Properties();

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config, "fenodes", "username", "password", "table_name", "db_name");
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        fenodes = config.getString("fenodes");
        username = config.getString("username");
        tableName = config.getString("table_name");
        password = config.getString("password");
        dbName = config.getString("db_name");
        if (config.hasPath("doris_sink_batch_size")) {
            batchSize = config.getInt("doris_sink_batch_size");
            Preconditions.checkArgument(batchSize > 0,"doris_sink_batch_size must be greater than 0");
        }
        if (config.hasPath("doris_sink_interval")) {
            batchIntervalMs = config.getInt("doris_sink_interval");
        }
        if (config.hasPath("doris_sink_max_retries")) {
            maxRetries = config.getInt("doris_sink_max_retries");
        }

        String producerPrefix = "doris_sink_properties.";
        PropertiesUtil.setProperties(config, streamLoadProp, producerPrefix, false);
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        batchIntervalMs = 0;
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        Table table = tableEnvironment.fromDataSet(dataSet);
        String[] fieldNames = table.getSchema().getFieldNames();

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(fenodes, dbName, tableName, username, password, streamLoadProp);
        return dataSet.output(new DorisOutputFormat<>(dorisStreamLoad, fieldNames, batchSize, batchIntervalMs, maxRetries));
    }

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.fromDataStream(dataStream);
        String[] fieldNames = table.getSchema().getFieldNames();

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(fenodes, dbName, tableName, username, password, streamLoadProp);
        dataStream.addSink(new DorisSinkFunction<>(new DorisOutputFormat<>(dorisStreamLoad, fieldNames, batchSize, batchIntervalMs, maxRetries)));
        return null;
    }
}

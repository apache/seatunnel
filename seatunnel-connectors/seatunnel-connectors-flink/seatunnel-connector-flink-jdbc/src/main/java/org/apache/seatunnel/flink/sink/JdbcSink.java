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

package org.apache.seatunnel.flink.sink;

import static org.apache.seatunnel.flink.Config.DRIVER;
import static org.apache.seatunnel.flink.Config.PASSWORD;
import static org.apache.seatunnel.flink.Config.QUERY;
import static org.apache.seatunnel.flink.Config.SINK_BATCH_INTERVAL;
import static org.apache.seatunnel.flink.Config.SINK_BATCH_MAX_RETRIES;
import static org.apache.seatunnel.flink.Config.SINK_BATCH_SIZE;
import static org.apache.seatunnel.flink.Config.URL;
import static org.apache.seatunnel.flink.Config.USERNAME;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Arrays;

public class JdbcSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private static final long serialVersionUID = 3677571223952518115L;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final int DEFAULT_MAX_RETRY_TIMES = 3;
    private static final int DEFAULT_INTERVAL_MILLIS = 0;
    private static final String PARALLELISM = "parallelism";

    private Config config;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private long batchIntervalMs = DEFAULT_INTERVAL_MILLIS;
    private int maxRetries = DEFAULT_MAX_RETRY_TIMES;

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
        return CheckConfigUtil.checkAllExists(config, DRIVER, URL, USERNAME, QUERY);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString(DRIVER);
        dbUrl = config.getString(URL);
        username = config.getString(USERNAME);
        query = config.getString(QUERY);
        if (config.hasPath(PASSWORD)) {
            password = config.getString(PASSWORD);
        }
        if (config.hasPath(SINK_BATCH_SIZE)) {
            batchSize = config.getInt(SINK_BATCH_SIZE);
        }
        if (config.hasPath(SINK_BATCH_INTERVAL)) {
            batchIntervalMs = config.getLong(SINK_BATCH_INTERVAL);
        }
        if (config.hasPath(SINK_BATCH_MAX_RETRIES)) {
            maxRetries = config.getInt(SINK_BATCH_MAX_RETRIES);
        }
    }

    @Override
    @Nullable
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        Table table = env.getStreamTableEnvironment().fromDataStream(dataStream);
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();

        int[] types = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
        SinkFunction<Row> sink = org.apache.flink.connector.jdbc.JdbcSink.sink(
            query,
            (st, row) -> JdbcUtils.setRecordToStatement(st, types, row),
            JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(batchIntervalMs)
                .withMaxRetries(maxRetries)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(dbUrl)
                .withDriverName(driverName)
                .withUsername(username)
                .withPassword(password)
                .build());

        if (config.hasPath(PARALLELISM)) {
            return dataStream.addSink(sink).setParallelism(config.getInt(PARALLELISM));
        }
        return dataStream.addSink(sink);
    }

    @Nullable
    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        Table table = env.getBatchTableEnvironment().fromDataSet(dataSet);
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();
        int[] types = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();

        JdbcOutputFormat format = JdbcOutputFormat.buildJdbcOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setBatchSize(batchSize)
                .setSqlTypes(types)
                .finish();
        return dataSet.output(format);
    }
}

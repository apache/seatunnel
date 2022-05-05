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

package org.apache.seatunnel.flink.jdbc.sink;

import static org.apache.seatunnel.flink.jdbc.Config.DRIVER;
import static org.apache.seatunnel.flink.jdbc.Config.PASSWORD;
import static org.apache.seatunnel.flink.jdbc.Config.QUERY;
import static org.apache.seatunnel.flink.jdbc.Config.SINK_BATCH_INTERVAL;
import static org.apache.seatunnel.flink.jdbc.Config.SINK_BATCH_MAX_RETRIES;
import static org.apache.seatunnel.flink.jdbc.Config.SINK_BATCH_SIZE;
import static org.apache.seatunnel.flink.jdbc.Config.SINK_IGNORE_POST_SQL_EXCEPTIONS;
import static org.apache.seatunnel.flink.jdbc.Config.SINK_POST_SQL;
import static org.apache.seatunnel.flink.jdbc.Config.SINK_PRE_SQL;
import static org.apache.seatunnel.flink.jdbc.Config.URL;
import static org.apache.seatunnel.flink.jdbc.Config.USERNAME;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class JdbcSink implements FlinkStreamSink, FlinkBatchSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSink.class);
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
    private String preSql;
    private String postSql;
    private boolean ignorePostSqlExceptions = false;
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
        if (config.hasPath(SINK_PRE_SQL)) {
            preSql = config.getString(SINK_PRE_SQL);
        }
        if (!env.isStreaming() && config.hasPath(SINK_POST_SQL)) {
            postSql = config.getString(SINK_POST_SQL);
        }
        if (config.hasPath(SINK_IGNORE_POST_SQL_EXCEPTIONS)) {
            ignorePostSqlExceptions = config.getBoolean(SINK_IGNORE_POST_SQL_EXCEPTIONS);
        }
    }

    @Override
    public String getPluginName() {
        return "JdbcSink";
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        executePreSql();

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
            dataStream.addSink(sink).setParallelism(config.getInt(PARALLELISM));
        } else {
            dataStream.addSink(sink);
        }
    }

    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        executePreSql();

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
        dataSet.output(format);
    }

    @Override
    public void close() throws Exception {
        executePostSql();
    }

    private void executePreSql() {
        if (!StringUtils.isNotBlank(preSql)) {
            LOGGER.info("Starting to execute pre sql: \n {}", preSql);
            try {
                executeSql(preSql);
            } catch (SQLException e) {
                LOGGER.error(String.format("Execute pre sql failed, pre sql is : \n %s \n", preSql), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void executePostSql() {
        if (!StringUtils.isNotBlank(postSql)) {
            LOGGER.info("Starting to execute post sql: \n {}", postSql);
            try {
                executeSql(postSql);
            } catch (SQLException e) {
                LOGGER.error(String.format("Execute post sql failed, post sql is : \n %s \n", postSql), e);
                if (!ignorePostSqlExceptions) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void executeSql(String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl, username, password);
            Statement statement = connection.createStatement()) {

            statement.execute(sql);
            LOGGER.info("Executed sql successfully.");
        }
    }
}

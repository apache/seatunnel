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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.List;

public class JdbcConfig implements Serializable {
    private static final int DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC = 30;
    private static final boolean DEFAULT_AUTO_COMMIT = true;

    public static final Option<String> URL = Options.key("url").stringType().noDefaultValue().withDescription("url");

    public static final Option<String> DRIVER =  Options.key("driver").stringType().noDefaultValue().withDescription("driver");

    public static final Option<Integer> CONNECTION_CHECK_TIMEOUT_SEC = Options.key("connection_check_timeout_sec").intType().defaultValue(DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC).withDescription("connection check time second");

    public static final Option<Integer> MAX_RETRIES = Options.key("max_retries").intType().noDefaultValue().withDescription("max_retired");

    public static final Option<String> USER = Options.key("user").stringType().noDefaultValue().withDescription("user");

    public static final Option<String> PASSWORD = Options.key("password").stringType().noDefaultValue().withDescription("password");

    public static final Option<String> QUERY = Options.key("query").stringType().noDefaultValue().withDescription("query");

    public static final Option<Boolean> AUTO_COMMIT = Options.key("auto_commit").booleanType().defaultValue(DEFAULT_AUTO_COMMIT).withDescription("auto commit");

    public static final Option<Integer> BATCH_SIZE = Options.key("batch_size").intType().noDefaultValue().withDescription("batch size");

    public static final Option<Integer> FETCH_SIZE = Options.key("fetch_size").intType().defaultValue(0).withDescription("For queries that return a large number of objects, " +
        "you can configure the row fetch size used in the query to improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.");

    public static final Option<Integer> BATCH_INTERVAL_MS = Options.key("batch_interval_ms").intType().noDefaultValue().withDescription("batch interval milliSecond");


    public static final Option<String> IS_EXACTLY_ONCE = Options.key("is_exactly_once").stringType().noDefaultValue().withDescription("exactly once");

    public static final Option<String> XA_DATA_SOURCE_CLASS_NAME = Options.key("xa_data_source_class_name").stringType().noDefaultValue().withDescription("data source class name");


    public static final Option<String> MAX_COMMIT_ATTEMPTS = Options.key("max_commit_attempts").stringType().noDefaultValue().withDescription("max commit attempts");

    public static final Option<String> TRANSACTION_TIMEOUT_SEC = Options.key("transaction_timeout_sec").stringType().noDefaultValue().withDescription("transaction timeout (second)");

    public static final Option<String> TABLE = Options.key("table").stringType().noDefaultValue().withDescription("table");

    public static final Option<List<String>> PRIMARY_KEYS = Options.key("primary_keys").listType().noDefaultValue().withDescription("primary keys");

    //source config
    public static final Option<String> PARTITION_COLUMN = Options.key("partition_column").stringType().noDefaultValue().withDescription("partition column");
    public static final Option<String> PARTITION_UPPER_BOUND = Options.key("partition_upper_bound").stringType().noDefaultValue().withDescription("partition upper bound");
    public static final Option<String> PARTITION_LOWER_BOUND = Options.key("partition_lower_bound").stringType().noDefaultValue().withDescription("partition lower bound");
    public static final Option<String> PARTITION_NUM = Options.key("partition_num").stringType().noDefaultValue().withDescription("partition num");

    public static JdbcConnectionOptions buildJdbcConnectionOptions(Config config) {

        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions();
        jdbcOptions.url = config.getString(JdbcConfig.URL.key());
        jdbcOptions.driverName = config.getString(JdbcConfig.DRIVER.key());
        if (config.hasPath(JdbcConfig.USER.key())) {
            jdbcOptions.username = config.getString(JdbcConfig.USER.key());
        }
        if (config.hasPath(JdbcConfig.PASSWORD.key())) {
            jdbcOptions.password = config.getString(JdbcConfig.PASSWORD.key());
        }

        if (config.hasPath(JdbcConfig.AUTO_COMMIT.key())) {
            jdbcOptions.autoCommit = config.getBoolean(JdbcConfig.AUTO_COMMIT.key());
        }

        if (config.hasPath(JdbcConfig.MAX_RETRIES.key())) {
            jdbcOptions.maxRetries = config.getInt(JdbcConfig.MAX_RETRIES.key());
        }
        if (config.hasPath(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC.key())) {
            jdbcOptions.connectionCheckTimeoutSeconds = config.getInt(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC.key());
        }
        if (config.hasPath(JdbcConfig.BATCH_SIZE.key())) {
            jdbcOptions.batchSize = config.getInt(JdbcConfig.BATCH_SIZE.key());
        }
        if (config.hasPath(JdbcConfig.BATCH_INTERVAL_MS.key())) {
            jdbcOptions.batchIntervalMs = config.getInt(JdbcConfig.BATCH_INTERVAL_MS.key());
        }

        if (config.hasPath(JdbcConfig.IS_EXACTLY_ONCE.key())) {
            jdbcOptions.xaDataSourceClassName = config.getString(JdbcConfig.XA_DATA_SOURCE_CLASS_NAME.key());
            if (config.hasPath(JdbcConfig.MAX_COMMIT_ATTEMPTS.key())) {
                jdbcOptions.maxCommitAttempts = config.getInt(JdbcConfig.MAX_COMMIT_ATTEMPTS.key());
            }
            if (config.hasPath(JdbcConfig.TRANSACTION_TIMEOUT_SEC.key())) {
                jdbcOptions.transactionTimeoutSec = config.getInt(JdbcConfig.TRANSACTION_TIMEOUT_SEC.key());
            }
        }

        return jdbcOptions;
    }
}

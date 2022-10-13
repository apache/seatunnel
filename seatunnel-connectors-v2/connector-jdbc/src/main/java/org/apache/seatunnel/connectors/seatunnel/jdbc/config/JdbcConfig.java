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

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

public class JdbcConfig implements Serializable {

    public static final String URL = "url";

    public static final String DRIVER = "driver";

    public static final String CONNECTION_CHECK_TIMEOUT_SEC = "connection_check_timeout_sec";

    public static final String MAX_RETRIES = "max_retries";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    public static final String QUERY = "query";

    public static final String BATCH_SIZE = "batch_size";

    public static final String BATCH_INTERVAL_MS = "batch_interval_ms";


    public static final String IS_EXACTLY_ONCE = "is_exactly_once";

    public static final String XA_DATA_SOURCE_CLASS_NAME = "xa_data_source_class_name";


    public static final String MAX_COMMIT_ATTEMPTS = "max_commit_attempts";

    public static final String TRANSACTION_TIMEOUT_SEC = "transaction_timeout_sec";

    public static final String TYPE_AFFINITY = "type_affinity";

    //source config
    public static final String PARTITION_COLUMN = "partition_column";
    public static final String PARTITION_UPPER_BOUND = "partition_upper_bound";
    public static final String PARTITION_LOWER_BOUND = "partition_lower_bound";
    public static final String PARTITION_NUM = "partition_num";

    public static JdbcConnectionOptions buildJdbcConnectionOptions(Config config) {

        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions();
        jdbcOptions.url = config.getString(JdbcConfig.URL);
        jdbcOptions.driverName = config.getString(JdbcConfig.DRIVER);
        if (config.hasPath(JdbcConfig.USER)) {
            jdbcOptions.username = config.getString(JdbcConfig.USER);
        }
        if (config.hasPath(JdbcConfig.PASSWORD)) {
            jdbcOptions.password = config.getString(JdbcConfig.PASSWORD);
        }
        jdbcOptions.query = config.getString(JdbcConfig.QUERY);

        if (config.hasPath(JdbcConfig.MAX_RETRIES)) {
            jdbcOptions.maxRetries = config.getInt(JdbcConfig.MAX_RETRIES);
        }
        if (config.hasPath(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC)) {
            jdbcOptions.connectionCheckTimeoutSeconds = config.getInt(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC);
        }
        if (config.hasPath(JdbcConfig.BATCH_SIZE)) {
            jdbcOptions.batchSize = config.getInt(JdbcConfig.BATCH_SIZE);
        }
        if (config.hasPath(JdbcConfig.BATCH_INTERVAL_MS)) {
            jdbcOptions.batchIntervalMs = config.getInt(JdbcConfig.BATCH_INTERVAL_MS);
        }

        if (config.hasPath(JdbcConfig.IS_EXACTLY_ONCE)) {
            jdbcOptions.xaDataSourceClassName = config.getString(JdbcConfig.XA_DATA_SOURCE_CLASS_NAME);
            if (config.hasPath(JdbcConfig.MAX_COMMIT_ATTEMPTS)) {
                jdbcOptions.maxCommitAttempts = config.getInt(JdbcConfig.MAX_COMMIT_ATTEMPTS);
            }
            if (config.hasPath(JdbcConfig.TRANSACTION_TIMEOUT_SEC)) {
                jdbcOptions.transactionTimeoutSec = config.getInt(JdbcConfig.TRANSACTION_TIMEOUT_SEC);
            }
        }

        if (config.hasPath(JdbcConfig.TYPE_AFFINITY)) {
            jdbcOptions.typeAffinity = config.getBoolean(JdbcConfig.TYPE_AFFINITY);
        }
        return jdbcOptions;
    }
}

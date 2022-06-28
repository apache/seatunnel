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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.NonNull;

import java.io.Serializable;
import java.util.Optional;

public class JdbcConnectorOptions
    implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC = 30;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_BATCH_SIZE = 300;
    private static final int DEFAULT_BATCH_INTERVAL_MS = 1000;
    private static final boolean DEFAULT_IS_EXACTLY_ONCE = false;
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 3;
    private static final int DEFAULT_TRANSACTION_TIMEOUT_SEC = -1;

    private String url;
    private String driverName;
    private int connectionCheckTimeoutSeconds = DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private String username;
    private String password;
    private String query;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;

    private boolean isExactlyOnce = DEFAULT_IS_EXACTLY_ONCE;
    private String xaDataSourceClassName;

    private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

    private int transactionTimeoutSec = DEFAULT_TRANSACTION_TIMEOUT_SEC;

    private JdbcConnectorOptions() {
    }

    public JdbcConnectorOptions(@NonNull Config config) {
        this.url = config.getString(JdbcConfig.URL);
        this.driverName = config.getString(JdbcConfig.DRIVER);
        if (config.hasPath(JdbcConfig.USER)) {
            this.username = config.getString(JdbcConfig.USER);
        }
        if (config.hasPath(JdbcConfig.PASSWORD)) {
            this.password = config.getString(JdbcConfig.PASSWORD);
        }
        this.query = config.getString(JdbcConfig.QUERY);

        if (config.hasPath(JdbcConfig.MAX_RETRIES)) {
            this.maxRetries = config.getInt(JdbcConfig.MAX_RETRIES);
        }
        if (config.hasPath(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC)) {
            this.connectionCheckTimeoutSeconds = config.getInt(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC);
        }
        if (config.hasPath(JdbcConfig.BATCH_SIZE)) {
            this.batchSize = config.getInt(JdbcConfig.BATCH_SIZE);
        }
        if (config.hasPath(JdbcConfig.BATCH_INTERVAL_MS)) {
            this.batchIntervalMs = config.getInt(JdbcConfig.BATCH_INTERVAL_MS);
        }

        if (config.hasPath(JdbcConfig.IS_EXACTLY_ONCE)) {
            this.isExactlyOnce = true;
            this.xaDataSourceClassName = config.getString(JdbcConfig.XA_DATA_SOURCE_CLASS_NAME);
            if (config.hasPath(JdbcConfig.MAX_COMMIT_ATTEMPTS)) {
                this.maxCommitAttempts = config.getInt(JdbcConfig.MAX_COMMIT_ATTEMPTS);
            }
            if (config.hasPath(JdbcConfig.TRANSACTION_TIMEOUT_SEC)) {
                this.transactionTimeoutSec = config.getInt(JdbcConfig.TRANSACTION_TIMEOUT_SEC);
            }
        }
    }

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public String getQuery() {
        return query;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public boolean isExactlyOnce() {
        return isExactlyOnce;
    }

    public String getXaDataSourceClassName() {
        return xaDataSourceClassName;
    }

    public int getMaxCommitAttempts() {
        return maxCommitAttempts;
    }

    public Optional<Integer> getTransactionTimeoutSec() {
        return transactionTimeoutSec < 0 ? Optional.empty() : Optional.of(transactionTimeoutSec);
    }

    public static JdbcConnectorOptionsBuilder builder() {
        return new JdbcConnectorOptionsBuilder();
    }

    public static final class JdbcConnectorOptionsBuilder {
        private String url;
        private String driverName;
        private int connectionCheckTimeoutSeconds = DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private String username;
        private String password;
        private String query;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;
        private boolean isExactlyOnce = DEFAULT_IS_EXACTLY_ONCE;
        private String xaDataSourceClassName;
        private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
        private int transactionTimeoutSec = DEFAULT_TRANSACTION_TIMEOUT_SEC;

        private JdbcConnectorOptionsBuilder() {
        }

        public JdbcConnectorOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public JdbcConnectorOptionsBuilder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public JdbcConnectorOptionsBuilder withConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public JdbcConnectorOptionsBuilder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public JdbcConnectorOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public JdbcConnectorOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public JdbcConnectorOptionsBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public JdbcConnectorOptionsBuilder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public JdbcConnectorOptionsBuilder withBatchIntervalMs(int batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public JdbcConnectorOptionsBuilder withIsExactlyOnce(boolean isExactlyOnce) {
            this.isExactlyOnce = isExactlyOnce;
            return this;
        }

        public JdbcConnectorOptionsBuilder withXaDataSourceClassName(String xaDataSourceClassName) {
            this.xaDataSourceClassName = xaDataSourceClassName;
            return this;
        }

        public JdbcConnectorOptionsBuilder withMaxCommitAttempts(int maxCommitAttempts) {
            this.maxCommitAttempts = maxCommitAttempts;
            return this;
        }

        public JdbcConnectorOptionsBuilder withTransactionTimeoutSec(int transactionTimeoutSec) {
            this.transactionTimeoutSec = transactionTimeoutSec;
            return this;
        }

        public JdbcConnectorOptions build() {
            JdbcConnectorOptions jdbcConnectorOptions = new JdbcConnectorOptions();
            jdbcConnectorOptions.batchSize = this.batchSize;
            jdbcConnectorOptions.batchIntervalMs = this.batchIntervalMs;
            jdbcConnectorOptions.driverName = this.driverName;
            jdbcConnectorOptions.maxRetries = this.maxRetries;
            jdbcConnectorOptions.password = this.password;
            jdbcConnectorOptions.connectionCheckTimeoutSeconds = this.connectionCheckTimeoutSeconds;
            jdbcConnectorOptions.query = this.query;
            jdbcConnectorOptions.url = this.url;
            jdbcConnectorOptions.username = this.username;
            jdbcConnectorOptions.transactionTimeoutSec = this.transactionTimeoutSec;
            jdbcConnectorOptions.maxCommitAttempts = this.maxCommitAttempts;
            jdbcConnectorOptions.isExactlyOnce = this.isExactlyOnce;
            jdbcConnectorOptions.xaDataSourceClassName = this.xaDataSourceClassName;
            return jdbcConnectorOptions;
        }
    }
}

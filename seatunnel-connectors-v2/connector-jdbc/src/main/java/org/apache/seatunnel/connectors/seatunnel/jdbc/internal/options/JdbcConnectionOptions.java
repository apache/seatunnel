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

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConfig;

import java.io.Serializable;
import java.util.Optional;

public class JdbcConnectionOptions
    implements Serializable {
    private static final long serialVersionUID = 1L;


    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_BATCH_SIZE = 300;
    private static final int DEFAULT_BATCH_INTERVAL_MS = 1000;
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 3;
    private static final int DEFAULT_TRANSACTION_TIMEOUT_SEC = -1;

    public String url;
    public String driverName;
    public int connectionCheckTimeoutSeconds = JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
    public int maxRetries = DEFAULT_MAX_RETRIES;
    public String username;
    public String password;
    public String query;

    public boolean autoCommit = JdbcConfig.AUTO_COMMIT.defaultValue();

    public int batchSize = DEFAULT_BATCH_SIZE;
    public int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;

    public String xaDataSourceClassName;

    public int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

    public int transactionTimeoutSec = DEFAULT_TRANSACTION_TIMEOUT_SEC;

    public JdbcConnectionOptions() {
    }

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public boolean isAutoCommit() {
        return autoCommit;
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

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchIntervalMs() {
        return batchIntervalMs;
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

    public static JdbcConnectionOptionsBuilder builder() {
        return new JdbcConnectionOptionsBuilder();
    }

    public static final class JdbcConnectionOptionsBuilder {
        private String url;
        private String driverName;
        private int connectionCheckTimeoutSeconds = JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private String username;
        private String password;
        private String query;
        private boolean autoCommit = JdbcConfig.AUTO_COMMIT.defaultValue();
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;
        private String xaDataSourceClassName;
        private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
        private int transactionTimeoutSec = DEFAULT_TRANSACTION_TIMEOUT_SEC;

        private JdbcConnectionOptionsBuilder() {
        }

        public JdbcConnectionOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public JdbcConnectionOptionsBuilder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public JdbcConnectionOptionsBuilder withConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public JdbcConnectionOptionsBuilder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public JdbcConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public JdbcConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public JdbcConnectionOptionsBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public JdbcConnectionOptionsBuilder withAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public JdbcConnectionOptionsBuilder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public JdbcConnectionOptionsBuilder withBatchIntervalMs(int batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public JdbcConnectionOptionsBuilder withXaDataSourceClassName(String xaDataSourceClassName) {
            this.xaDataSourceClassName = xaDataSourceClassName;
            return this;
        }

        public JdbcConnectionOptionsBuilder withMaxCommitAttempts(int maxCommitAttempts) {
            this.maxCommitAttempts = maxCommitAttempts;
            return this;
        }

        public JdbcConnectionOptionsBuilder withTransactionTimeoutSec(int transactionTimeoutSec) {
            this.transactionTimeoutSec = transactionTimeoutSec;
            return this;
        }

        public JdbcConnectionOptions build() {
            JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions();
            jdbcConnectionOptions.batchSize = this.batchSize;
            jdbcConnectionOptions.batchIntervalMs = this.batchIntervalMs;
            jdbcConnectionOptions.driverName = this.driverName;
            jdbcConnectionOptions.maxRetries = this.maxRetries;
            jdbcConnectionOptions.password = this.password;
            jdbcConnectionOptions.connectionCheckTimeoutSeconds = this.connectionCheckTimeoutSeconds;
            jdbcConnectionOptions.url = this.url;
            jdbcConnectionOptions.autoCommit = this.autoCommit;
            jdbcConnectionOptions.username = this.username;
            jdbcConnectionOptions.transactionTimeoutSec = this.transactionTimeoutSec;
            jdbcConnectionOptions.maxCommitAttempts = this.maxCommitAttempts;
            jdbcConnectionOptions.xaDataSourceClassName = this.xaDataSourceClassName;
            return jdbcConnectionOptions;
        }
    }
}

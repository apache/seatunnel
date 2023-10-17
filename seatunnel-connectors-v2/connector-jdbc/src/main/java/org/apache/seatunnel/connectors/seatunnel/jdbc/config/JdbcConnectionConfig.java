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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JdbcConnectionConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    public String url;
    public String driverName;
    public String compatibleMode;
    public int connectionCheckTimeoutSeconds =
            JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
    public int maxRetries = JdbcOptions.MAX_RETRIES.defaultValue();
    public String username;
    public String password;
    public String query;

    public boolean autoCommit = JdbcOptions.AUTO_COMMIT.defaultValue();

    public int batchSize = JdbcOptions.BATCH_SIZE.defaultValue();

    public String xaDataSourceClassName;

    public int maxCommitAttempts = JdbcOptions.MAX_COMMIT_ATTEMPTS.defaultValue();

    public int transactionTimeoutSec = JdbcOptions.TRANSACTION_TIMEOUT_SEC.defaultValue();

    private Map<String, String> properties;

    public static JdbcConnectionConfig of(ReadonlyConfig config) {
        JdbcConnectionConfig.Builder builder = JdbcConnectionConfig.builder();
        builder.url(config.get(JdbcOptions.URL));
        builder.compatibleMode(config.get(JdbcOptions.COMPATIBLE_MODE));
        builder.driverName(config.get(JdbcOptions.DRIVER));
        builder.autoCommit(config.get(JdbcOptions.AUTO_COMMIT));
        builder.maxRetries(config.get(JdbcOptions.MAX_RETRIES));
        builder.connectionCheckTimeoutSeconds(config.get(JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC));
        builder.batchSize(config.get(JdbcOptions.BATCH_SIZE));
        if (config.get(JdbcOptions.IS_EXACTLY_ONCE)) {
            builder.xaDataSourceClassName(config.get(JdbcOptions.XA_DATA_SOURCE_CLASS_NAME));
            builder.maxCommitAttempts(config.get(JdbcOptions.MAX_COMMIT_ATTEMPTS));
            builder.transactionTimeoutSec(config.get(JdbcOptions.TRANSACTION_TIMEOUT_SEC));
            builder.maxRetries(0);
        }

        config.getOptional(JdbcOptions.USER).ifPresent(builder::username);
        config.getOptional(JdbcOptions.PASSWORD).ifPresent(builder::password);
        config.getOptional(JdbcOptions.PROPERTIES).ifPresent(builder::properties);
        return builder.build();
    }

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getCompatibleMode() {
        return compatibleMode;
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

    public String getXaDataSourceClassName() {
        return xaDataSourceClassName;
    }

    public int getMaxCommitAttempts() {
        return maxCommitAttempts;
    }

    public Optional<Integer> getTransactionTimeoutSec() {
        return transactionTimeoutSec < 0 ? Optional.empty() : Optional.of(transactionTimeoutSec);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static JdbcConnectionConfig.Builder builder() {
        return new JdbcConnectionConfig.Builder();
    }

    public static final class Builder {
        private String url;
        private String driverName;
        private String compatibleMode;
        private int connectionCheckTimeoutSeconds =
                JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
        private int maxRetries = JdbcOptions.MAX_RETRIES.defaultValue();
        private String username;
        private String password;
        private String query;
        private boolean autoCommit = JdbcOptions.AUTO_COMMIT.defaultValue();
        private int batchSize = JdbcOptions.BATCH_SIZE.defaultValue();
        private String xaDataSourceClassName;
        private int maxCommitAttempts = JdbcOptions.MAX_COMMIT_ATTEMPTS.defaultValue();
        private int transactionTimeoutSec = JdbcOptions.TRANSACTION_TIMEOUT_SEC.defaultValue();
        private Map<String, String> properties;

        private Builder() {}

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder driverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder compatibleMode(String compatibleMode) {
            this.compatibleMode = compatibleMode;
            return this;
        }

        public Builder connectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder xaDataSourceClassName(String xaDataSourceClassName) {
            this.xaDataSourceClassName = xaDataSourceClassName;
            return this;
        }

        public Builder maxCommitAttempts(int maxCommitAttempts) {
            this.maxCommitAttempts = maxCommitAttempts;
            return this;
        }

        public Builder transactionTimeoutSec(int transactionTimeoutSec) {
            this.transactionTimeoutSec = transactionTimeoutSec;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public JdbcConnectionConfig build() {
            JdbcConnectionConfig jdbcConnectionConfig = new JdbcConnectionConfig();
            jdbcConnectionConfig.batchSize = this.batchSize;
            jdbcConnectionConfig.driverName = this.driverName;
            jdbcConnectionConfig.compatibleMode = this.compatibleMode;
            jdbcConnectionConfig.maxRetries = this.maxRetries;
            jdbcConnectionConfig.password = this.password;
            jdbcConnectionConfig.connectionCheckTimeoutSeconds = this.connectionCheckTimeoutSeconds;
            jdbcConnectionConfig.url = this.url;
            jdbcConnectionConfig.autoCommit = this.autoCommit;
            jdbcConnectionConfig.username = this.username;
            jdbcConnectionConfig.transactionTimeoutSec = this.transactionTimeoutSec;
            jdbcConnectionConfig.maxCommitAttempts = this.maxCommitAttempts;
            jdbcConnectionConfig.xaDataSourceClassName = this.xaDataSourceClassName;
            jdbcConnectionConfig.properties =
                    this.properties == null ? new HashMap<>() : this.properties;
            return jdbcConnectionConfig;
        }
    }
}

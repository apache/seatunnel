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

import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Getter
public class JdbcConnectionConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private String url;
    private String driverName;
    private String compatibleMode;
    private int connectionCheckTimeoutSeconds =
            JdbcBaseOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
    private int maxRetries = JdbcSinkOptions.MAX_RETRIES.defaultValue();
    private String username;
    private String password;
    private String query;

    private boolean autoCommit = JdbcSinkOptions.AUTO_COMMIT.defaultValue();

    private int batchSize = JdbcSinkOptions.BATCH_SIZE.defaultValue();

    private String xaDataSourceClassName;

    private boolean decimalTypeNarrowing = JdbcSourceOptions.DECIMAL_TYPE_NARROWING.defaultValue();

    private int maxCommitAttempts = JdbcSinkOptions.MAX_COMMIT_ATTEMPTS.defaultValue();

    private int transactionTimeoutSec = JdbcSinkOptions.TRANSACTION_TIMEOUT_SEC.defaultValue();

    private boolean useKerberos = JdbcSourceOptions.USE_KERBEROS.defaultValue();

    private String kerberosPrincipal;

    private String kerberosKeytabPath;

    private String krb5Path = JdbcSourceOptions.KRB5_PATH.defaultValue();

    private String dialect = JdbcBaseOptions.DIALECT.defaultValue();

    private Map<String, String> properties;

    public static JdbcConnectionConfig of(ReadonlyConfig config) {
        JdbcConnectionConfig.Builder builder = JdbcConnectionConfig.builder();
        builder.url(config.get(JdbcBaseOptions.URL));
        builder.compatibleMode(config.get(JdbcBaseOptions.COMPATIBLE_MODE));
        builder.driverName(config.get(JdbcBaseOptions.DRIVER));
        builder.autoCommit(config.get(JdbcSinkOptions.AUTO_COMMIT));
        builder.maxRetries(config.get(JdbcSinkOptions.MAX_RETRIES));
        builder.connectionCheckTimeoutSeconds(
                config.get(JdbcBaseOptions.CONNECTION_CHECK_TIMEOUT_SEC));
        builder.batchSize(config.get(JdbcSinkOptions.BATCH_SIZE));
        if (config.get(JdbcSinkOptions.IS_EXACTLY_ONCE)) {
            builder.xaDataSourceClassName(config.get(JdbcSinkOptions.XA_DATA_SOURCE_CLASS_NAME));
            builder.maxCommitAttempts(config.get(JdbcSinkOptions.MAX_COMMIT_ATTEMPTS));
            builder.transactionTimeoutSec(config.get(JdbcSinkOptions.TRANSACTION_TIMEOUT_SEC));
            builder.maxRetries(0);
        }
        if (config.get(JdbcBaseOptions.USE_KERBEROS)) {
            builder.useKerberos(config.get(JdbcBaseOptions.USE_KERBEROS));
            builder.kerberosPrincipal(config.get(JdbcBaseOptions.KERBEROS_PRINCIPAL));
            builder.kerberosKeytabPath(config.get(JdbcBaseOptions.KERBEROS_KEYTAB_PATH));
            builder.krb5Path(config.get(JdbcBaseOptions.KRB5_PATH));
        }
        config.getOptional(JdbcBaseOptions.USER).ifPresent(builder::username);
        config.getOptional(JdbcBaseOptions.PASSWORD).ifPresent(builder::password);
        config.getOptional(JdbcSourceOptions.PROPERTIES).ifPresent(builder::properties);
        config.getOptional(JdbcSourceOptions.DECIMAL_TYPE_NARROWING)
                .ifPresent(builder::decimalTypeNarrowing);
        config.getOptional(JdbcBaseOptions.DIALECT).ifPresent(builder::dialect);
        return builder.build();
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public Optional<Integer> getTransactionTimeoutSec() {
        return transactionTimeoutSec < 0 ? Optional.empty() : Optional.of(transactionTimeoutSec);
    }

    public static JdbcConnectionConfig.Builder builder() {
        return new JdbcConnectionConfig.Builder();
    }

    public static final class Builder {
        private String url;
        private String driverName;
        private String compatibleMode;
        private int connectionCheckTimeoutSeconds =
                JdbcBaseOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
        private int maxRetries = JdbcSinkOptions.MAX_RETRIES.defaultValue();
        private String username;
        private String password;
        private String query;
        private boolean autoCommit = JdbcSinkOptions.AUTO_COMMIT.defaultValue();
        private int batchSize = JdbcSinkOptions.BATCH_SIZE.defaultValue();
        private String xaDataSourceClassName;
        private boolean decimalTypeNarrowing =
                JdbcSourceOptions.DECIMAL_TYPE_NARROWING.defaultValue();
        private int maxCommitAttempts = JdbcSinkOptions.MAX_COMMIT_ATTEMPTS.defaultValue();
        private int transactionTimeoutSec = JdbcSinkOptions.TRANSACTION_TIMEOUT_SEC.defaultValue();
        private Map<String, String> properties;
        public boolean useKerberos = JdbcSourceOptions.USE_KERBEROS.defaultValue();
        public String kerberosPrincipal;
        public String kerberosKeytabPath;
        public String krb5Path = JdbcBaseOptions.KRB5_PATH.defaultValue();
        public String dialect = JdbcBaseOptions.DIALECT.defaultValue();

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

        public Builder decimalTypeNarrowing(boolean decimalTypeNarrowing) {
            this.decimalTypeNarrowing = decimalTypeNarrowing;
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

        public Builder useKerberos(boolean useKerberos) {
            this.useKerberos = useKerberos;
            return this;
        }

        public Builder kerberosPrincipal(String kerberosPrincipal) {
            this.kerberosPrincipal = kerberosPrincipal;
            return this;
        }

        public Builder kerberosKeytabPath(String kerberosKeytabPath) {
            this.kerberosKeytabPath = kerberosKeytabPath;
            return this;
        }

        public Builder krb5Path(String krb5Path) {
            this.krb5Path = krb5Path;
            return this;
        }

        public Builder dialect(String dialect) {
            this.dialect = dialect;
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
            jdbcConnectionConfig.decimalTypeNarrowing = this.decimalTypeNarrowing;
            jdbcConnectionConfig.useKerberos = this.useKerberos;
            jdbcConnectionConfig.kerberosPrincipal = this.kerberosPrincipal;
            jdbcConnectionConfig.kerberosKeytabPath = this.kerberosKeytabPath;
            jdbcConnectionConfig.krb5Path = this.krb5Path;
            jdbcConnectionConfig.dialect = this.dialect;
            jdbcConnectionConfig.properties =
                    this.properties == null ? new HashMap<>() : this.properties;
            return jdbcConnectionConfig;
        }
    }
}

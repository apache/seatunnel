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

package org.apache.seatunnel.connectors.cdc.base.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import lombok.Setter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** A {@link SourceConfig.Factory} to provide {@link SourceConfig} of JDBC data source. */
public abstract class JdbcSourceConfigFactory implements SourceConfig.Factory<JdbcSourceConfig> {

    private static final long serialVersionUID = 1L;

    protected int port;
    protected String hostname;
    protected String username;
    protected String password;
    protected String originUrl;
    protected List<String> databaseList;
    protected List<String> tableList;
    protected String databasePattern;
    protected String tablePattern;
    protected StartupConfig startupConfig;
    protected StopConfig stopConfig;
    protected double distributionFactorUpper =
            JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue();
    protected double distributionFactorLower =
            JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue();
    protected int sampleShardingThreshold =
            JdbcSourceOptions.SAMPLE_SHARDING_THRESHOLD.defaultValue();
    protected int inverseSamplingRate = JdbcSourceOptions.INVERSE_SAMPLING_RATE.defaultValue();
    protected int splitSize = SourceOptions.SNAPSHOT_SPLIT_SIZE.defaultValue();
    protected Map<String, String> splitColumn;
    protected int fetchSize = SourceOptions.SNAPSHOT_FETCH_SIZE.defaultValue();
    protected String serverTimeZone = JdbcSourceOptions.SERVER_TIME_ZONE.defaultValue();
    protected long connectTimeoutMillis = JdbcSourceOptions.CONNECT_TIMEOUT_MS.defaultValue();
    protected int connectMaxRetries = JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue();
    protected int connectionPoolSize = JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue();
    @Setter protected boolean exactlyOnce = JdbcSourceOptions.EXACTLY_ONCE.defaultValue();

    @Setter
    protected boolean schemaChangeEnabled = JdbcSourceOptions.SCHEMA_CHANGES_ENABLED.defaultValue();

    protected Properties dbzProperties;

    /** String hostname of the database server. */
    public JdbcSourceConfigFactory hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public JdbcSourceConfigFactory splitColumn(Map<String, String> splitColumn) {
        this.splitColumn = splitColumn;
        return this;
    }

    /** Integer port number of the database server. */
    public JdbcSourceConfigFactory port(int port) {
        this.port = port;
        return this;
    }

    public JdbcSourceConfigFactory originUrl(String originUrl) {
        this.originUrl = originUrl;
        return this;
    }

    /**
     * An optional list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring. By default all
     * databases will be monitored.
     */
    public JdbcSourceConfigFactory databaseList(String... databaseList) {
        this.databaseList = Arrays.asList(databaseList);
        return this;
    }

    /**
     * An optional list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form databaseName.tableName. by default the connector will monitor
     * every non-system table in each monitored database.
     */
    public JdbcSourceConfigFactory tableList(String... tableList) {
        this.tableList = Arrays.asList(tableList);
        return this;
    }

    /** Name of the user to use when connecting to the database server. */
    public JdbcSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    /** Password to use when connecting to the database server. */
    public JdbcSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type converted to STRING. See more
     * https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types
     */
    public JdbcSourceConfigFactory serverTimeZone(String timeZone) {
        this.serverTimeZone = timeZone;
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public JdbcSourceConfigFactory splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public JdbcSourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        this.distributionFactorUpper = distributionFactorUpper;
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public JdbcSourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        this.distributionFactorLower = distributionFactorLower;
        return this;
    }

    /**
     * The threshold for the row count to trigger sample-based sharding strategy. When the
     * distribution factor is within the upper and lower bounds, if the approximate row count
     * exceeds this threshold, the sample-based sharding strategy will be used. This can help to
     * handle large datasets in a more efficient manner.
     *
     * @param sampleShardingThreshold The threshold of row count.
     * @return This JdbcSourceConfigFactory.
     */
    public JdbcSourceConfigFactory sampleShardingThreshold(int sampleShardingThreshold) {
        this.sampleShardingThreshold = sampleShardingThreshold;
        return this;
    }

    /**
     * The inverse of the sampling rate to be used for data sharding based on sampling. The actual
     * sampling rate is 1 / inverseSamplingRate. For instance, if inverseSamplingRate is 1000, then
     * the sampling rate is 1/1000, meaning every 1000th record will be included in the sample used
     * for sharding.
     *
     * @param inverseSamplingRate The value representing the inverse of the desired sampling rate.
     * @return this JdbcSourceConfigFactory instance.
     */
    public JdbcSourceConfigFactory inverseSamplingRate(int inverseSamplingRate) {
        this.inverseSamplingRate = inverseSamplingRate;
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public JdbcSourceConfigFactory fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the database
     * server before timing out.
     */
    public JdbcSourceConfigFactory connectTimeoutMillis(long connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    /** The connection pool size. */
    public JdbcSourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /** The max retry times to get connection. */
    public JdbcSourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        this.connectMaxRetries = connectMaxRetries;
        return this;
    }

    /** Whether the {@link SourceConfig} should output the schema changes or not. */
    public JdbcSourceConfigFactory schemaChangeEnabled(boolean schemaChangeEnabled) {
        this.schemaChangeEnabled = schemaChangeEnabled;
        return this;
    }

    /** The Debezium connector properties. For example, "snapshot.mode". */
    public JdbcSourceConfigFactory debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    /** Specifies the startup options. */
    public JdbcSourceConfigFactory startupOptions(StartupConfig startupConfig) {
        this.startupConfig = startupConfig;
        return this;
    }

    /** Specifies the stop options. */
    public JdbcSourceConfigFactory stopOptions(StopConfig stopConfig) {
        this.stopConfig = stopConfig;
        return this;
    }

    public JdbcSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        this.port = config.get(JdbcSourceOptions.PORT);
        this.hostname = config.get(JdbcSourceOptions.HOSTNAME);
        this.username = config.get(JdbcSourceOptions.USERNAME);
        this.password = config.get(JdbcSourceOptions.PASSWORD);
        this.databaseList = config.get(JdbcSourceOptions.DATABASE_NAMES);
        this.tableList = config.get(ConnectorCommonOptions.TABLE_NAMES);
        this.databasePattern = config.get(ConnectorCommonOptions.DATABASE_PATTERN);
        this.tablePattern = config.get(ConnectorCommonOptions.TABLE_PATTERN);
        this.distributionFactorUpper =
                config.get(JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        this.distributionFactorLower =
                config.get(JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        this.sampleShardingThreshold = config.get(JdbcSourceOptions.SAMPLE_SHARDING_THRESHOLD);
        this.inverseSamplingRate = config.get(JdbcSourceOptions.INVERSE_SAMPLING_RATE);
        this.splitSize = config.get(SourceOptions.SNAPSHOT_SPLIT_SIZE);
        this.splitColumn = new HashMap<>();
        config.getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG)
                .ifPresent(
                        jtcs -> {
                            jtcs.forEach(
                                    jtc -> {
                                        this.splitColumn.put(
                                                jtc.getTable(), jtc.getSnapshotSplitColumn());
                                    });
                        });

        this.fetchSize = config.get(SourceOptions.SNAPSHOT_FETCH_SIZE);
        this.serverTimeZone = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        this.connectTimeoutMillis = config.get(JdbcSourceOptions.CONNECT_TIMEOUT_MS);
        this.connectMaxRetries = config.get(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        this.connectionPoolSize = config.get(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        this.exactlyOnce = config.get(JdbcSourceOptions.EXACTLY_ONCE);
        this.schemaChangeEnabled = config.get(JdbcSourceOptions.SCHEMA_CHANGES_ENABLED);
        this.dbzProperties = new Properties();
        config.getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                .ifPresent(map -> dbzProperties.putAll(map));
        return this;
    }

    @Override
    public abstract JdbcSourceConfig create(int subtask);
}

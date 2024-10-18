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

import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;

import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.util.List;
import java.util.Properties;

/**
 * A Source configuration which is used by {@link IncrementalSource} which used JDBC data source.
 */
public abstract class JdbcSourceConfig extends BaseSourceConfig {

    protected final String driverClassName;
    protected final String hostname;
    protected final int port;
    protected final String username;
    protected final String password;
    protected final String originUrl;
    protected final List<String> databaseList;
    protected final List<String> tableList;
    protected final int fetchSize;
    protected final String serverTimeZone;
    protected final long connectTimeoutMillis;
    protected final int connectMaxRetries;
    protected final int connectionPoolSize;

    public JdbcSourceConfig(
            StartupConfig startupConfig,
            StopConfig stopConfig,
            List<String> databaseList,
            List<String> tableList,
            int splitSize,
            Properties splitColumn,
            double distributionFactorUpper,
            double distributionFactorLower,
            int sampleShardingThreshold,
            int inverseSamplingRate,
            Properties dbzProperties,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            String originUrl,
            int fetchSize,
            String serverTimeZone,
            long connectTimeoutMillis,
            int connectMaxRetries,
            int connectionPoolSize,
            boolean exactlyOnce) {
        super(
                startupConfig,
                stopConfig,
                splitSize,
                splitColumn,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                exactlyOnce,
                dbzProperties);
        this.driverClassName = driverClassName;
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.originUrl = originUrl;
        this.databaseList = databaseList;
        this.tableList = tableList;
        this.fetchSize = fetchSize;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
    }

    public abstract RelationalDatabaseConnectorConfig getDbzConnectorConfig();

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getOriginUrl() {
        return originUrl;
    }

    public String getPassword() {
        return password;
    }

    public List<String> getDatabaseList() {
        return databaseList;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public long getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public int getConnectMaxRetries() {
        return connectMaxRetries;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }
}

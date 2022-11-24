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

package org.apache.seatunnel.connectors.cdc.base.relational.connection;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/** A connection pool factory to create pooled DataSource {@link HikariDataSource}. */
public abstract class JdbcConnectionPoolFactory {

    public static final String CONNECTION_POOL_PREFIX = "connection-pool-";
    public static final String SERVER_TIMEZONE_KEY = "serverTimezone";
    public static final int MINIMUM_POOL_SIZE = 1;

    public HikariDataSource createPooledDataSource(JdbcSourceConfig sourceConfig) {
        final HikariConfig config = new HikariConfig();

        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();

        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);
        config.setJdbcUrl(getJdbcUrl(sourceConfig));
        config.setUsername(sourceConfig.getUsername());
        config.setPassword(sourceConfig.getPassword());
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(sourceConfig.getConnectionPoolSize());
        config.setConnectionTimeout(sourceConfig.getConnectTimeout().toMillis());
        config.addDataSourceProperty(SERVER_TIMEZONE_KEY, sourceConfig.getServerTimeZone());
        config.setDriverClassName(sourceConfig.getDriverClassName());

        // optional optimization configurations for pooled DataSource
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }

    /**
     * due to relational database url of the forms are different, e.g. Mysql <code>
     * jdbc:mysql://<em>hostname</em>:<em>port</em></code>, Oracle Thin <code>
     * jdbc:oracle:thin:@<em>hostname</em>:<em>port</em>:<em>dbName</em></code> DB2 <code>
     * jdbc:db2://<em>hostname</em>:<em>port</em>/<em>dbName</em></code> Sybase <code>
     * jdbc:sybase:Tds:<em>hostname</em>:<em>port</em></code>, so generate a jdbc url by specific
     * database.
     *
     * @param sourceConfig a basic Source configuration.
     * @return a database url.
     */
    public abstract String getJdbcUrl(JdbcSourceConfig sourceConfig);
}

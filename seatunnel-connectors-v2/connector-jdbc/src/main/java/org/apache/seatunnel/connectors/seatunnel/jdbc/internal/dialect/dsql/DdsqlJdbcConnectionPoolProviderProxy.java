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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dsql;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DdsqlJdbcConnectionPoolProviderProxy implements JdbcConnectionProvider {

    private final transient DsqlConnectionPoolManager poolManager;
    private final JdbcConnectionConfig jdbcConfig;
    private final int queueIndex;

    public DdsqlJdbcConnectionPoolProviderProxy(JdbcConnectionConfig jdbcConfig, int queueIndex) {

        this.jdbcConfig = jdbcConfig;
        this.poolManager = new DsqlConnectionPoolManager(jdbcConfig);
        this.queueIndex = queueIndex;
    }

    @Override
    public Connection getConnection() {
        return poolManager.getConnection(queueIndex);
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return poolManager.containsConnection(queueIndex)
                && poolManager
                        .getConnection(queueIndex)
                        .isValid(jdbcConfig.getConnectionCheckTimeoutSeconds());
    }

    @Override
    public Connection getOrEstablishConnection() {
        return poolManager.getConnection(queueIndex);
    }

    @Override
    public void closeConnection() {
        if (poolManager.containsConnection(queueIndex)) {
            try {
                poolManager.remove(queueIndex).close();
            } catch (SQLException e) {
                log.warn("JDBC connection close failed.", e);
            }
        }
    }

    @Override
    public Connection reestablishConnection() {
        closeConnection();
        return getOrEstablishConnection();
    }
}

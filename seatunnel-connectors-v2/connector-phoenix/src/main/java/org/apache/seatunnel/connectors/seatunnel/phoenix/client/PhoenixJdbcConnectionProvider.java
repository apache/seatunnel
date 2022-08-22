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

package org.apache.seatunnel.connectors.seatunnel.phoenix.client;

import org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PhoenixJdbcConnectionProvider {
    private final PhoenixSinkConfig sinkConfig;
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixJdbcConnectionProvider.class);

    public PhoenixJdbcConnectionProvider(PhoenixSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    private transient Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && connection.isValid(sinkConfig.getConnectionCheckTimeoutSeconds());
    }

    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (connection != null) {
            return connection;
        }
        connection = getJdbcConnection(sinkConfig);

        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new SQLException(
                    "No suitable driver found for " + sinkConfig.getConnectionString(), "08001");
        }

        return connection;
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }

    public static Connection getJdbcConnection(PhoenixSinkConfig sinkConfig) throws SQLException, ClassNotFoundException {
        String connStr = sinkConfig.getConnectionString();
        LOG.debug("Connecting to HBase cluster [" + connStr + "] ...");
        Connection conn;

        if (sinkConfig.isThinClient()) {
            conn = getThinClientJdbcConnection(sinkConfig);
        } else {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(connStr);
        }
        conn.setAutoCommit(false);

        LOG.debug("Connected to HBase cluster successfully.");
        return conn;
    }

    public static Connection getThinClientJdbcConnection(PhoenixSinkConfig sinkConfig) throws SQLException, ClassNotFoundException {
        String connStr = sinkConfig.getConnectionString();
        LOG.debug("Connecting to HBase cluster [" + connStr + "] use thin client ...");
        Properties properties = new Properties();
        if (StringUtils.isEmpty(sinkConfig.getUsername())) {
            properties.setProperty(PhoenixSinkConfig.JDBC_USER, sinkConfig.getPassword());
        }
        if (StringUtils.isEmpty(sinkConfig.getPassword())) {
            properties.setProperty(PhoenixSinkConfig.JDBC_PASSWORD, sinkConfig.getPassword());
        }
        Class.forName("org.apache.phoenix.queryserver.client.Driver");
        return DriverManager.getConnection(connStr, properties);
    }
}

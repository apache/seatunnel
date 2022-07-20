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

package org.apache.seatunnel.connectors.seatunnel.tidb.connection;

import org.apache.seatunnel.connectors.seatunnel.tidb.config.JdbcConnectionOptions;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Simple JDBC connection provider.
 */
public class SimpleJdbcConnectionProvider
    implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;

    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    public SimpleJdbcConnectionProvider(@NonNull JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid()
        throws SQLException {
        return connection != null
            && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException {
        if (connection != null) {
            return connection;
        }
        Driver driver = DriverManager.getDriver(jdbcOptions.getUrl());
        Properties info = new Properties();
        if (jdbcOptions.getUsername().isPresent()) {
            info.setProperty("user", jdbcOptions.getUsername().get());
        }
        if (jdbcOptions.getPassword().isPresent()) {
            info.setProperty("password", jdbcOptions.getPassword().get());
        }
        connection = driver.connect(jdbcOptions.getUrl(), info);
        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new SQLException(
                "No suitable driver found for " + jdbcOptions.getUrl(), "08001");
        }

        return connection;
    }

    @Override
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

    @Override
    public Connection reestablishConnection()
        throws SQLException {
        closeConnection();
        return getOrEstablishConnection();
    }
}

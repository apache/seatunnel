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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/** Utility methods for JDBC driver-specific connection validation hooks. */
public final class JdbcConnectionValidationUtils {

    /** Xugu JDBC driver class name used by connector-jdbc ITs. */
    public static final String XUGU_DRIVER = "com.xugu.cloudjdbc.Driver";

    /** Validation query used when the Xugu driver cannot answer Connection.isValid(timeout). */
    public static final String XUGU_VALIDATION_QUERY = "SELECT 1 FROM DUAL";

    private JdbcConnectionValidationUtils() {}

    /**
     * Xugu's driver throws during Connection.isValid(timeout), so pooled connections need a SQL
     * probe instead of the JDBC driver validation hook.
     */
    public static boolean isConnectionValid(Connection connection, JdbcConnectionConfig jdbcConfig)
            throws SQLException {
        if (connection == null) {
            return false;
        }

        Optional<String> validationQuery = getConnectionValidationQuery(jdbcConfig);
        if (!validationQuery.isPresent()) {
            return connection.isValid(jdbcConfig.getConnectionCheckTimeoutSeconds());
        }

        try (PreparedStatement preparedStatement =
                        connection.prepareStatement(validationQuery.get());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            return resultSet.next();
        }
    }

    /**
     * Returns an optional validation query for drivers that need SQL-based liveness checks instead
     * of {@link Connection#isValid(int)}.
     */
    public static Optional<String> getConnectionValidationQuery(JdbcConnectionConfig jdbcConfig) {
        if (jdbcConfig == null) {
            return Optional.empty();
        }

        String driverName = jdbcConfig.getDriverName();
        String url = jdbcConfig.getUrl();
        if (XUGU_DRIVER.equals(driverName) || (url != null && url.startsWith("jdbc:xugu:"))) {
            return Optional.of(XUGU_VALIDATION_QUERY);
        }

        return Optional.empty();
    }
}

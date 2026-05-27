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

package org.apache.seatunnel.edge.agent.starter.command.db;

import org.apache.seatunnel.edge.agent.starter.wal.sqlite.SqliteSchemaBootstrap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class EdgeAgentDbConnection implements AutoCloseable {

    private final Connection connection;

    private EdgeAgentDbConnection(Connection connection) {
        this.connection = connection;
    }

    public static EdgeAgentDbConnection openReadOnly(Path sqlitePath) throws SQLException {
        if (!Files.isRegularFile(sqlitePath)) {
            throw new SQLException("SQLite database file not found: " + sqlitePath);
        }
        openMaintenanceConnection(sqlitePath).close();
        Connection connection =
                DriverManager.getConnection(
                        "jdbc:sqlite:" + sqlitePath.toAbsolutePath().normalize());
        try (java.sql.Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA query_only = ON");
        }
        return new EdgeAgentDbConnection(connection);
    }

    public static EdgeAgentDbConnection openReadWrite(Path sqlitePath) throws SQLException {
        return new EdgeAgentDbConnection(openMaintenanceConnection(sqlitePath));
    }

    private static Connection openMaintenanceConnection(Path sqlitePath) throws SQLException {
        Path prepared = SqliteSchemaBootstrap.prepareSqlitePath(sqlitePath);
        Connection connection =
                DriverManager.getConnection("jdbc:sqlite:" + prepared.toAbsolutePath());
        SqliteSchemaBootstrap.applyConnectionPragmas(connection);
        SqliteSchemaBootstrap.initRuntimeSchema(connection);
        return connection;
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}

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

package org.apache.seatunnel.edge.agent.starter.wal;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

public class WalStoreFactory {

    /**
     * Opens a shared SQLite database for WAL outbound rows and input source positions.
     *
     * <p>Called from {@code EdgeAgentComponentAssembler.assemble}. Relative {@code sqlite-path}
     * values are resolved against {@code workingDirectory}.
     *
     * @param sqlitePath path from {@code queue.sqlite-path}
     * @param workingDirectory agent install or process working directory
     * @return store exposing {@link WalStore} and {@link
     *     org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore}
     * @throws SQLException if the database cannot be opened or migrated
     */
    public static SqliteAgentRuntimeStore openRuntime(String sqlitePath, Path workingDirectory)
            throws SQLException {
        return SqliteAgentRuntimeStore.open(resolveSqlitePath(sqlitePath, workingDirectory));
    }

    public static Path resolveSqlitePath(String sqlitePath, Path workingDirectory) {
        Objects.requireNonNull(sqlitePath, "sqlitePath");
        Path path = Paths.get(sqlitePath);
        if (path.isAbsolute()) {
            return path;
        }
        return Objects.requireNonNull(workingDirectory, "workingDirectory").resolve(path);
    }

    /**
     * Opens a read-write JDBC connection and ensures WAL + source-position schemas exist.
     *
     * <p>Caller must close the JDBC connection. Intended for maintenance CLIs when the agent
     * process is stopped.
     */
    public static Connection openMaintenanceConnection(Path sqlitePath) throws SQLException {
        Path prepared = SqliteSchemaBootstrap.prepareSqlitePath(sqlitePath);
        Connection connection =
                DriverManager.getConnection("jdbc:sqlite:" + prepared.toAbsolutePath());
        SqliteSchemaBootstrap.applyConnectionPragmas(connection);
        SqliteSchemaBootstrap.initRuntimeSchema(connection);
        return connection;
    }
}

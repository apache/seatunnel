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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

final class SqliteSchemaBootstrap {

    static Path prepareSqlitePath(Path sqlitePath) {
        Path parent = sqlitePath.getParent();
        if (parent != null) {
            parent.toFile().mkdirs();
        }
        return sqlitePath;
    }

    static void applyConnectionPragmas(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=NORMAL");
        }
    }

    static void initWalSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(WalSqlStatements.CREATE_TABLE);
            statement.execute(WalSqlStatements.CREATE_INDEX_STATUS_ID);
            statement.execute(WalSqlStatements.CREATE_INDEX_UPDATED_AT);
        }
        migrateWalBatchIdColumn(connection);
    }

    static void migrateWalBatchIdColumn(Connection connection) throws SQLException {
        if (hasWalColumn(connection)) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(WalSqlStatements.MIGRATE_ADD_BATCH_ID);
            statement.execute(WalSqlStatements.MIGRATE_BACKFILL_BATCH_ID);
        }
    }

    private static boolean hasWalColumn(Connection connection) throws SQLException {
        try (java.sql.PreparedStatement statement =
                        connection.prepareStatement(
                                String.format("PRAGMA table_info(%s)", WalSqlStatements.TABLE));
                java.sql.ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                if ("batch_id".equalsIgnoreCase(resultSet.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    static void initSourcePositionSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(SourcePositionSqlStatements.CREATE_TABLE);
        }
    }

    static void initMetaSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(AgentMetaSqlStatements.CREATE_TABLE);
        }
    }

    static void initRuntimeSchema(Connection connection) throws SQLException {
        applyConnectionPragmas(connection);
        initWalSchema(connection);
        initSourcePositionSchema(connection);
        initMetaSchema(connection);
    }
}

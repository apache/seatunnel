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

import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class SqliteSourcePositionStore implements EdgeSourcePositionStore, AutoCloseable {

    private final Connection connection;
    private final boolean closeConnection;

    public SqliteSourcePositionStore(Path sqlitePath) throws SQLException {
        Path prepared = SqliteSchemaBootstrap.prepareSqlitePath(sqlitePath);
        Connection connection =
                java.sql.DriverManager.getConnection("jdbc:sqlite:" + prepared.toAbsolutePath());
        SqliteSchemaBootstrap.applyConnectionPragmas(connection);
        SqliteSchemaBootstrap.initSourcePositionSchema(connection);
        this.connection = connection;
        this.closeConnection = true;
    }

    SqliteSourcePositionStore(Connection connection, boolean closeConnection) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.closeConnection = closeConnection;
    }

    @Override
    public EdgeSourcePosition load(String sourceId, String partition) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        SourcePositionSqlStatements.SELECT_BY_SOURCE_AND_PARTITION)) {
            statement.setString(1, sourceId);
            statement.setString(2, partition);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return toPosition(resultSet);
                }
            }
        }
        return null;
    }

    @Override
    public Map<String, EdgeSourcePosition> loadBySource(String sourceId) throws Exception {
        Map<String, EdgeSourcePosition> positions = new LinkedHashMap<>();
        try (PreparedStatement statement =
                connection.prepareStatement(SourcePositionSqlStatements.SELECT_BY_SOURCE)) {
            statement.setString(1, sourceId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    EdgeSourcePosition position = toPosition(resultSet);
                    positions.put(position.getPartition(), position);
                }
            }
        }
        return positions;
    }

    @Override
    public void save(EdgeSourcePosition position) throws Exception {
        String sourceId = position.getSourceId();
        String partition = position.getPartition();
        if (sourceId == null || sourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("source position sourceId must not be blank");
        }
        if (partition == null || partition.trim().isEmpty()) {
            throw new IllegalArgumentException("source position partition must not be blank");
        }
        long updatedAt =
                position.getUpdatedAt() > 0L ? position.getUpdatedAt() : System.currentTimeMillis();
        try (PreparedStatement statement =
                connection.prepareStatement(SourcePositionSqlStatements.UPSERT)) {
            statement.setString(1, sourceId);
            statement.setString(2, partition);
            statement.setLong(3, position.getOffset());
            statement.setBytes(4, MetadataSerde.serialize(position.getMetadata()));
            statement.setLong(5, updatedAt);
            statement.executeUpdate();
        }
    }

    @Override
    public void close() throws SQLException {
        if (closeConnection) {
            connection.close();
        }
    }

    private EdgeSourcePosition toPosition(ResultSet resultSet) throws Exception {
        return EdgeSourcePosition.builder()
                .sourceId(resultSet.getString("source_id"))
                .partition(resultSet.getString("partition_key"))
                .offset(resultSet.getLong("offset_value"))
                .metadata(MetadataSerde.deserialize(resultSet.getBytes("metadata")))
                .updatedAt(resultSet.getLong("updated_at"))
                .build();
    }
}

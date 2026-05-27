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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqliteBatchIdAllocator {

    /**
     * Returns the next batch id and advances the counter. Must run inside an open transaction on
     * {@code connection}.
     */
    public static long allocateNext(Connection connection) throws SQLException {
        Long current = readNextBatchId(connection);
        if (current == null) {
            seedMetaFromWal(connection);
            current = readNextBatchId(connection);
        }
        if (current == null || current <= 0) {
            throw new SQLException("Failed to initialize next_batch_id in edge_agent_meta");
        }
        try (PreparedStatement statement =
                connection.prepareStatement(AgentMetaSqlStatements.UPDATE_VALUE)) {
            statement.setLong(1, current + 1);
            statement.setString(2, AgentMetaSqlStatements.KEY_NEXT_BATCH_ID);
            if (statement.executeUpdate() != 1) {
                throw new SQLException("Failed to advance next_batch_id");
            }
        }
        return current;
    }

    public static Long peekNextBatchId(Connection connection) throws SQLException {
        return readNextBatchId(connection);
    }

    public static void seedMetaFromWal(Connection connection) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(AgentMetaSqlStatements.SEED_NEXT_BATCH_ID_FROM_WAL)) {
            statement.executeUpdate();
        }
    }

    private static Long readNextBatchId(Connection connection) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(AgentMetaSqlStatements.SELECT_VALUE)) {
            statement.setString(1, AgentMetaSqlStatements.KEY_NEXT_BATCH_ID);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
            }
        }
        return null;
    }
}

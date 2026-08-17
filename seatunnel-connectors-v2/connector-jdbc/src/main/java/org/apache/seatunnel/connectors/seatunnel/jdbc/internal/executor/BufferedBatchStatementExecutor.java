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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class BufferedBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> statementExecutor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;
    @NonNull private final List<SeaTunnelRow> buffer = new ArrayList<>();
    private boolean flushFailed;

    public BufferedBatchStatementExecutor(
            @NonNull JdbcBatchStatementExecutor<SeaTunnelRow> statementExecutor,
            @NonNull Function<SeaTunnelRow, SeaTunnelRow> valueTransform) {
        this.statementExecutor = statementExecutor;
        this.valueTransform = valueTransform;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statementExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        buffer.add(valueTransform.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!buffer.isEmpty()) {
            try {
                for (SeaTunnelRow row : buffer) {
                    statementExecutor.addToBatch(row);
                }
                statementExecutor.executeBatch();
                buffer.clear();
                flushFailed = false;
            } catch (SQLException e) {
                flushFailed = true;
                throw e;
            }
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            if (!buffer.isEmpty() && !flushFailed) {
                executeBatch();
            }
        } finally {
            statementExecutor.closeStatements();
        }
    }
}

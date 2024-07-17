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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.executor;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

@RequiredArgsConstructor
public class InsertOrUpdateBatchStatementExecutor implements JdbcBatchStatementExecutor {
    private final StatementFactory existStmtFactory;
    @NonNull private final StatementFactory insertStmtFactory;
    @NonNull private final StatementFactory updateStmtFactory;
    private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    private final JdbcRowConverter keyRowConverter;
    @NonNull private final JdbcRowConverter valueRowConverter;
    private transient PreparedStatement existStatement;
    private transient PreparedStatement insertStatement;
    private transient PreparedStatement updateStatement;
    private transient Boolean preChangeFlag;
    private transient boolean submitted;

    public InsertOrUpdateBatchStatementExecutor(
            StatementFactory insertStmtFactory,
            StatementFactory updateStmtFactory,
            JdbcRowConverter rowConverter) {
        this(null, insertStmtFactory, updateStmtFactory, null, null, rowConverter);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        if (upsertMode()) {
            existStatement = existStmtFactory.createStatement(connection);
        }
        insertStatement = insertStmtFactory.createStatement(connection);
        updateStatement = updateStmtFactory.createStatement(connection);
    }

    private boolean upsertMode() {
        return existStmtFactory != null;
    }

    private boolean hasInsert(SeaTunnelRow record) throws SQLException {
        if (upsertMode()) {
            return !exist(keyExtractor.apply(record));
        }
        switch (record.getRowKind()) {
            case INSERT:
                return true;
            case UPDATE_AFTER:
                return false;
            default:
                // todo
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        boolean currentChangeFlag = hasInsert(record);
        if (currentChangeFlag) {
            if (preChangeFlag != null && !preChangeFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            }
            valueRowConverter.toExternal(record, insertStatement);
            insertStatement.addBatch();
        } else {
            if (preChangeFlag != null && preChangeFlag) {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }
            valueRowConverter.toExternal(record, updateStatement);
            updateStatement.addBatch();
        }
        preChangeFlag = currentChangeFlag;
        submitted = false;
    }

    @Override
    public void executeBatch() throws SQLException {
        if (preChangeFlag != null) {
            if (preChangeFlag) {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            } else {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            }
        }
        submitted = true;
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!submitted) {
            executeBatch();
        }
        for (PreparedStatement statement :
                Arrays.asList(existStatement, insertStatement, updateStatement)) {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private boolean exist(SeaTunnelRow pk) throws SQLException {
        keyRowConverter.toExternal(pk, existStatement);
        try (ResultSet resultSet = existStatement.executeQuery()) {
            return resultSet.next();
        }
    }
}

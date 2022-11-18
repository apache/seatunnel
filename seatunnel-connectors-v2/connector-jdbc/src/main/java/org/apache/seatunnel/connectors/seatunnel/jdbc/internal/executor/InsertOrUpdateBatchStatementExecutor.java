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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

@RequiredArgsConstructor
public class InsertOrUpdateBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull
    private final StatementFactory existStmtFactory;
    @NonNull
    private final StatementFactory insertStmtFactory;
    @NonNull
    private final StatementFactory updateStmtFactory;
    @NonNull
    private final SeaTunnelRowType valueRowType;
    @NonNull
    private final SeaTunnelRowType keyRowType;
    @NonNull
    private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull
    private final JdbcRowConverter rowConverter;

    private transient PreparedStatement existStatement;
    private transient PreparedStatement insertStatement;
    private transient PreparedStatement updateStatement;
    private transient Boolean preExistFlag;
    private transient boolean submitted;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        existStatement = existStmtFactory.createStatement(connection);
        insertStatement = insertStmtFactory.createStatement(connection);
        updateStatement = updateStmtFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        boolean currentExistFlag = exist(keyExtractor.apply(record));
        if (currentExistFlag) {
            if (preExistFlag != null && !preExistFlag) {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }
            rowConverter.toExternal(valueRowType, record, updateStatement);
            updateStatement.addBatch();
        } else {
            if (preExistFlag != null && preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            }
            rowConverter.toExternal(valueRowType, record, insertStatement);
            insertStatement.addBatch();
        }
        preExistFlag = currentExistFlag;
        submitted = false;
    }

    @Override
    public void executeBatch() throws SQLException {
        if (preExistFlag != null) {
            if (preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            } else {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }
        }
        submitted = true;
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!submitted) {
            executeBatch();
        }
        for (PreparedStatement statement : Arrays.asList(existStatement, insertStatement, updateStatement)) {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private boolean exist(SeaTunnelRow pk) throws SQLException {
        rowConverter.toExternal(keyRowType, pk, existStatement);
        try (ResultSet resultSet = existStatement.executeQuery()) {
            return resultSet.next();
        }
    }
}

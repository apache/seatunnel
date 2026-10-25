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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Triple;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.function.Function;

@RequiredArgsConstructor
public class BufferReducedBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor;
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;

    /**
     * Buffered rows by key, so that only the last change of a key is written. The value holds the
     * change flag, the key row and the value row. A key with a NULL field is buffered under a
     * unique identity instead, see {@link #bufferKey(SeaTunnelRow)}.
     */
    @NonNull private final LinkedHashMap<Object, Triple<Boolean, SeaTunnelRow, SeaTunnelRow>> buffer =
            new LinkedHashMap<>();

    private boolean flushFailed;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        upsertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        if (RowKind.UPDATE_BEFORE.equals(record.getRowKind())) {
            // do nothing
            return;
        }

        SeaTunnelRow key = keyExtractor.apply(record);
        boolean changeFlag = changeFlag(record.getRowKind());
        SeaTunnelRow value = valueTransform.apply(record);
        buffer.put(bufferKey(key), Triple.of(changeFlag, key, value));
    }

    /**
     * Returns the key a row is buffered under. In SQL a NULL key value never equals another value,
     * so the database treats rows with a NULL key field (possible for a key taken from a nullable
     * unique key) as different rows, and an upsert or delete by that key never matches them. Such
     * rows get a unique identity so that they are not merged into one row within a batch.
     */
    private static Object bufferKey(SeaTunnelRow key) {
        for (Object field : key.getFields()) {
            if (field == null) {
                return new Object();
            }
        }
        return key;
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            Boolean preChangeFlag = null;
            for (Triple<Boolean, SeaTunnelRow, SeaTunnelRow> bufferedRow : buffer.values()) {
                Boolean currentChangeFlag = bufferedRow.getLeft();
                if (currentChangeFlag) {
                    if (preChangeFlag != null && !preChangeFlag) {
                        deleteExecutor.executeBatch();
                    }
                    upsertExecutor.addToBatch(bufferedRow.getRight());
                } else {
                    if (preChangeFlag != null && preChangeFlag) {
                        upsertExecutor.executeBatch();
                    }
                    deleteExecutor.addToBatch(bufferedRow.getMiddle());
                }
                preChangeFlag = currentChangeFlag;
            }

            if (preChangeFlag != null) {
                if (preChangeFlag) {
                    upsertExecutor.executeBatch();
                } else {
                    deleteExecutor.executeBatch();
                }
            }
            buffer.clear();
            flushFailed = false;
        } catch (SQLException e) {
            flushFailed = true;
            throw e;
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            if (!buffer.isEmpty() && !flushFailed) {
                executeBatch();
            }
        } finally {
            upsertExecutor.closeStatements();
            deleteExecutor.closeStatements();
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        buffer.clear();
        upsertExecutor.clearBatch();
        deleteExecutor.clearBatch();
    }

    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported rowKind: " + rowKind);
        }
    }
}

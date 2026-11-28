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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Applies every change of a batch in arrival order, used by {@code update_condition_sql}. Unlike
 * {@link BufferReducedBatchStatementExecutor}, a key is not reduced to its last change.
 *
 * <p>The pending upsert batch is executed before a key repeats. PostgreSQL fails a statement that
 * changes the same row twice, and a driver that rewrites a batch into one statement can meet keys
 * that are equal for the database but not here, so then each row is executed on its own.
 */
@RequiredArgsConstructor
public class OrderedBufferedBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    /** Takes whole rows; this executor prepares, runs and closes it. */
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor;

    /** Takes key rows only; this executor prepares, runs and closes it. */
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor;

    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;

    /** Set when the driver may merge a batch of upserts into one statement. */
    private final boolean oneRowPerStatement;

    /** Kept until a flush succeeds, so a retry replays every change. */
    @NonNull private final List<SeaTunnelRow> buffer = new ArrayList<>();

    /** After a failed flush, closing must not send the buffer again. */
    private boolean flushFailed;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        upsertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        if (RowKind.UPDATE_BEFORE.equals(record.getRowKind())) {
            return;
        }
        // Fail on an unsupported row kind when it arrives, not when the batch is flushed.
        isUpsert(record.getRowKind());
        buffer.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            // Drop statements left by a failed attempt, as the whole buffer is replayed.
            upsertExecutor.clearBatch();
            deleteExecutor.clearBatch();

            Set<SeaTunnelRow> keysInUpsertBatch = new HashSet<>();
            Boolean pendingUpsert = null;
            for (SeaTunnelRow record : buffer) {
                boolean upsert = isUpsert(record.getRowKind());
                if (pendingUpsert != null && pendingUpsert != upsert) {
                    executePending(pendingUpsert);
                    keysInUpsertBatch.clear();
                }
                SeaTunnelRow key = keyExtractor.apply(record);
                if (upsert) {
                    // NULL usually never equals NULL in SQL, so such a key is not tracked.
                    if (!hasNullField(key) && !keysInUpsertBatch.add(key)) {
                        upsertExecutor.executeBatch();
                        keysInUpsertBatch.clear();
                        keysInUpsertBatch.add(key);
                    }
                    upsertExecutor.addToBatch(valueTransform.apply(record));
                    if (oneRowPerStatement) {
                        upsertExecutor.executeBatch();
                        keysInUpsertBatch.clear();
                    }
                } else {
                    deleteExecutor.addToBatch(key);
                }
                pendingUpsert = upsert;
            }
            if (pendingUpsert != null) {
                executePending(pendingUpsert);
            }
            buffer.clear();
            flushFailed = false;
        } catch (SQLException e) {
            flushFailed = true;
            throw e;
        }
    }

    private void executePending(boolean upsert) throws SQLException {
        if (upsert) {
            upsertExecutor.executeBatch();
        } else {
            deleteExecutor.executeBatch();
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

    private static boolean hasNullField(SeaTunnelRow key) {
        for (Object field : key.getFields()) {
            if (field == null) {
                return true;
            }
        }
        return false;
    }

    private static boolean isUpsert(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
                return false;
            default:
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported rowKind: " + rowKind);
        }
    }
}

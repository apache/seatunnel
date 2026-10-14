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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import lombok.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class BufferReducedBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor;
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;
    private final boolean keyOrdered;

    @NonNull private final LinkedHashMap<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> buffer =
            new LinkedHashMap<>();

    private boolean flushFailed;

    public BufferReducedBatchStatementExecutor(
            @NonNull JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor,
            @NonNull JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor,
            @NonNull Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            @NonNull Function<SeaTunnelRow, SeaTunnelRow> valueTransform) {
        this(upsertExecutor, deleteExecutor, keyExtractor, valueTransform, false);
    }

    public BufferReducedBatchStatementExecutor(
            @NonNull JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor,
            @NonNull JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor,
            @NonNull Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            @NonNull Function<SeaTunnelRow, SeaTunnelRow> valueTransform,
            boolean keyOrdered) {
        this.upsertExecutor = upsertExecutor;
        this.deleteExecutor = deleteExecutor;
        this.keyExtractor = keyExtractor;
        this.valueTransform = valueTransform;
        this.keyOrdered = keyOrdered;
    }

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
        buffer.put(key, Pair.of(changeFlag, value));
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            Boolean preChangeFlag = null;
            Collection<Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>>> entries =
                    buffer.entrySet();
            if (keyOrdered && buffer.size() > 1) {
                List<Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>>> sorted =
                        new ArrayList<>(buffer.entrySet());
                sorted.sort(
                        Map.Entry.comparingByKey(BufferReducedBatchStatementExecutor::compareKeys));
                entries = sorted;
            }
            for (Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> entry : entries) {
                Boolean currentChangeFlag = entry.getValue().getKey();
                if (currentChangeFlag) {
                    if (preChangeFlag != null && !preChangeFlag) {
                        deleteExecutor.executeBatch();
                    }
                    upsertExecutor.addToBatch(entry.getValue().getValue());
                } else {
                    if (preChangeFlag != null && preChangeFlag) {
                        upsertExecutor.executeBatch();
                    }
                    deleteExecutor.addToBatch(entry.getKey());
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

    /**
     * Compares primary-key rows using a deterministic lexicographic order. The type name is part of
     * the comparison when two fields have different Java types, which keeps the comparator
     * transitive even when malformed input mixes numeric and textual values.
     */
    static int compareKeys(SeaTunnelRow left, SeaTunnelRow right) {
        int arity = Math.min(left.getArity(), right.getArity());
        for (int i = 0; i < arity; i++) {
            Object a = left.getField(i);
            Object b = right.getField(i);
            int comparison = compareFields(a, b);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.getArity(), right.getArity());
    }

    private static int compareFields(Object left, Object right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof byte[] && right instanceof byte[]) {
            return compareBytes((byte[]) left, (byte[]) right);
        }

        int typeComparison = left.getClass().getName().compareTo(right.getClass().getName());
        if (typeComparison != 0) {
            return typeComparison;
        }
        if (left instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> comparable = (Comparable<Object>) left;
            int valueComparison = comparable.compareTo(right);
            if (valueComparison != 0) {
                return valueComparison;
            }
        }
        return 0;
    }

    private static int compareBytes(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int comparison = Integer.compare(left[i] & 0xFF, right[i] & 0xFF);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            if (!buffer.isEmpty() && !flushFailed) {
                executeBatch();
            } else if (flushFailed) {
                // A child executor may still contain a partially submitted JDBC batch. Never let
                // its close method execute that batch after the transaction has failed.
                upsertExecutor.clearBatch();
                deleteExecutor.clearBatch();
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

    @Override
    public boolean supportsTransactionConflictRetry() {
        return true;
    }

    @Override
    public void resetForTransactionConflictRetry() throws SQLException {
        upsertExecutor.clearBatch();
        deleteExecutor.clearBatch();
        flushFailed = false;
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

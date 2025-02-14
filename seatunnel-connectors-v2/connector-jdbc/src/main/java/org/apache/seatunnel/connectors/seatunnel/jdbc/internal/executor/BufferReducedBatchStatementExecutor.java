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

import org.apache.commons.lang3.tuple.Pair;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@RequiredArgsConstructor
public class BufferReducedBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor;
    @NonNull private final JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;

    @NonNull private final LinkedHashMap<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> buffer =
            new LinkedHashMap<>();

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
        Boolean preChangeFlag = null;
        Set<Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>>> entrySet = buffer.entrySet();
        for (Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> entry : entrySet) {
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
    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            if (!buffer.isEmpty()) {
                executeBatch();
            }
        } finally {
            upsertExecutor.closeStatements();
            deleteExecutor.closeStatements();
        }
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

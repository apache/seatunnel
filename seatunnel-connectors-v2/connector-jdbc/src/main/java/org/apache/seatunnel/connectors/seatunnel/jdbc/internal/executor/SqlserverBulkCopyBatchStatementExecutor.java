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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class SqlserverBulkCopyBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {

    @NonNull private final String schemaTableName;
    @NonNull private final List<Column> columns;
    @NonNull private final List<Object[]> buffer = new ArrayList<>();

    private Connection connection;
    private ResultSetMetaData resultSetMetaData;

    public SqlserverBulkCopyBatchStatementExecutor(String schemaTableName, List<Column> columns) {
        this.columns = columns;
        this.schemaTableName = schemaTableName;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection.unwrap(com.microsoft.sqlserver.jdbc.SQLServerConnection.class);
        this.connection.setAutoCommit(false);
        this.resultSetMetaData = getResultSetMetaData(this.connection, schemaTableName);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        Object[] rowData = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Object field = record.getField(i);
            SeaTunnelDataType<?> type = columns.get(i).getDataType();
            switch (type.getSqlType()) {
                case DATE:
                    rowData[i] =
                            field == null
                                    ? null
                                    : java.sql.Date.valueOf((java.time.LocalDate) field);
                    break;
                case TIME:
                    rowData[i] =
                            field == null
                                    ? null
                                    : java.sql.Time.valueOf((java.time.LocalTime) field);
                    break;
                case TIMESTAMP:
                    rowData[i] =
                            field == null
                                    ? null
                                    : java.sql.Timestamp.valueOf((java.time.LocalDateTime) field);
                    break;
                default:
                    rowData[i] = field;
            }
        }
        buffer.add(rowData);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatchInternal();
        }
    }

    private void executeBatchInternal() {
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
            bulkCopy.setDestinationTableName(schemaTableName);
            // BulkCopy config
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setTableLock(true);
            options.setUseInternalTransaction(false);
            options.setCheckConstraints(false);
            options.setFireTriggers(false);
            options.setBatchSize(buffer.size());
            bulkCopy.setBulkCopyOptions(options);
            long start = System.currentTimeMillis();
            bulkCopy.writeToServer(new MemoryBulkData(resultSetMetaData, buffer));
            connection.commit();
            log.info(
                    "Bulk copied {} rows to table {}, cost {}s",
                    buffer.size(),
                    schemaTableName,
                    (System.currentTimeMillis() - start) / 1000);
            buffer.clear();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.error("Failed to rollback", rollbackEx);
            }
            // todo improve Exception
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED, e);
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        executeBatch();
    }

    private ResultSetMetaData getResultSetMetaData(Connection connection, String schemaTableName) {
        final String[] split = schemaTableName.split("\\.");
        if (split.length != 2) {
            throw new SeaTunnelRuntimeException(
                    JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED, "");
        }
        String queryMeta =
                String.format("select * from \"%s\".\"%s\" where 1=0", split[0], split[1]);
        try {
            return connection.createStatement().executeQuery(queryMeta).getMetaData();
        } catch (SQLException e) {
            // todo improve Exception
            throw new SeaTunnelRuntimeException(
                    JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED,
                    "get meta data fail:" + schemaTableName);
        }
    }

    static class MemoryBulkData implements ISQLServerBulkData {
        private final ResultSetMetaData metaData;
        private final Iterator<Object[]> iterator;
        private Object[] current;

        public MemoryBulkData(ResultSetMetaData metaData, List<Object[]> rows) {
            this.metaData = metaData;
            this.iterator = rows.iterator();
        }

        @SneakyThrows
        @Override
        public Set<Integer> getColumnOrdinals() {
            int columnCount = metaData.getColumnCount();
            Set<Integer> ordinals = new LinkedHashSet<>();
            for (int i = 1; i <= columnCount; i++) {
                ordinals.add(i);
            }
            return ordinals;
        }

        @Override
        public Object[] getRowData() {
            if (current == null) {
                // todo improve Exception
                throw new IllegalStateException(
                        "RowData requested but no current row. next() was not called.");
            }
            return current;
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            }
            return false;
        }

        @SneakyThrows
        @Override
        public String getColumnName(int column) {
            return metaData.getColumnName(column);
        }

        @SneakyThrows
        @Override
        public int getColumnType(int column) {
            return metaData.getColumnType(column);
        }

        @SneakyThrows
        @Override
        public int getPrecision(int column) {
            return metaData.getPrecision(column);
        }

        @SneakyThrows
        @Override
        public int getScale(int column) {
            return metaData.getScale(column);
        }
    }
}

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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Slf4j
public class SqlserverBulkCopyBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {

    @NonNull private final String schemaTableName;
    @NonNull private final List<Column> columns;
    @NonNull private final List<Object[]> buffer = new ArrayList<>();

    private Connection connection;

    public SqlserverBulkCopyBatchStatementExecutor(String schemaTableName, List<Column> columns) {
        this.columns = columns;
        this.schemaTableName = schemaTableName;
    }

    @SneakyThrows
    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection.unwrap(com.microsoft.sqlserver.jdbc.SQLServerConnection.class);
        this.connection.setAutoCommit(false);
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

    private void executeBatchInternal() throws SQLException {
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
            bulkCopy.writeToServer(new MemoryBulkData(columns, buffer));
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
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED, e);
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        executeBatch();
    }

    static class MemoryBulkData implements ISQLServerBulkData {
        private final List<Column> columns;
        private final Iterator<Object[]> iterator;
        private Object[] current;

        public MemoryBulkData(List<Column> columns, List<Object[]> rows) {
            this.columns = columns;
            this.iterator = rows.iterator();
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            Set<Integer> ordinals = new HashSet<>();
            for (int i = 1; i <= columns.size(); i++) ordinals.add(i);
            return ordinals;
        }

        @Override
        public Object[] getRowData() {
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

        @Override
        public String getColumnName(int column) {
            return columns.get(column - 1).getName();
        }

        @Override
        public int getColumnType(int column) {
            SeaTunnelDataType<?> type = columns.get(column - 1).getDataType();
            String columnName = columns.get(column - 1).getName();
            switch (type.getSqlType()) {
                case STRING:
                    return java.sql.Types.VARCHAR;
                case BOOLEAN:
                    return java.sql.Types.BIT;
                case TINYINT:
                    return java.sql.Types.TINYINT;
                case SMALLINT:
                    return java.sql.Types.SMALLINT;
                case INT:
                    return java.sql.Types.INTEGER;
                case BIGINT:
                    return java.sql.Types.BIGINT;
                case FLOAT:
                    return java.sql.Types.FLOAT;
                case DOUBLE:
                    return java.sql.Types.DOUBLE;
                case DECIMAL:
                    return java.sql.Types.DECIMAL;
                case DATE:
                    return java.sql.Types.DATE;
                case TIME:
                    return java.sql.Types.TIME;
                case TIMESTAMP:
                    return java.sql.Types.TIMESTAMP;
                case BYTES:
                    return java.sql.Types.VARBINARY;
                case NULL:
                    return java.sql.Types.NULL;
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected columnName: " + columnName);
            }
        }

        @Override
        public int getPrecision(int column) {
            final Column columnImpl = columns.get(column - 1);
            final SeaTunnelDataType<?> dataType = columnImpl.getDataType();
            final Long columnLength = columnImpl.getColumnLength();
            switch (dataType.getSqlType()) {
                case DECIMAL:
                    if (columnLength == null || columnLength <= 0) {
                        return 38;
                    }
                    return columnLength.intValue();
                case STRING:
                    return columnLength.intValue();
                case TIMESTAMP:
                case DATE:
                case TIME:
                    return 23;
                default:
                    if (columnLength == null || columnLength <= 0) {
                        return 0;
                    } else {
                        return columnLength.intValue();
                    }
            }
        }

        @Override
        public int getScale(int column) {
            final Column columnImpl = columns.get(column - 1);
            final Integer scale = columnImpl.getScale();
            return scale == null ? 0 : scale;
        }
    }
}

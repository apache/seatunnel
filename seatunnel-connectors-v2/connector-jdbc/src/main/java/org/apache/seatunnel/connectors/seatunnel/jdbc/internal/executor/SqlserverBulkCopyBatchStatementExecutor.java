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
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executes insert-only bulk copies within the connection transaction owned by the JDBC writer.
 *
 * <p>Failed batches remain buffered for the existing reconnect and retry path.
 */
public class SqlserverBulkCopyBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    private final String schemaTableName;
    private final List<Column> columns;
    private final List<Object[]> buffer = new ArrayList<>();
    private Connection connection;
    private List<ColumnMetadata> metadata;

    public SqlserverBulkCopyBatchStatementExecutor(String schemaTableName, List<Column> columns) {
        this.schemaTableName = schemaTableName;
        this.columns = columns;
    }

    /**
     * Refreshes destination metadata on initial connection and reconnect without owning a
     * transaction.
     *
     * <p>Pending rows survive reconnect because the JDBC writer reuses this executor for retries.
     */
    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
        // Do not change auto-commit: JdbcOutputFormat and its writer own transaction boundaries.
        this.metadata = readColumnMetadata();
    }

    /**
     * Buffers an INSERT after adapting SeaTunnel local temporal values to JDBC value types.
     *
     * <p>OffsetDateTime values remain unchanged and use JDBC TIMESTAMP_WITH_TIMEZONE metadata.
     */
    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        if (record.getRowKind() != RowKind.INSERT) {
            throw new SQLException(
                    "SQL Server bulk copy only accepts INSERT rows for table " + schemaTableName,
                    "0A000");
        }
        Object[] rowData = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Object field = record.getField(i);
            SeaTunnelDataType<?> type = columns.get(i).getDataType();
            switch (type.getSqlType()) {
                case DATE:
                    rowData[i] = field == null ? null : Date.valueOf((LocalDate) field);
                    break;
                case TIME:
                    // The native bulk TIME writer expects Timestamp, preserving fractional seconds.
                    rowData[i] =
                            field == null
                                    ? null
                                    : Timestamp.valueOf(
                                            LocalDate.of(1970, 1, 1).atTime((LocalTime) field));
                    break;
                case TIMESTAMP:
                    rowData[i] = field == null ? null : Timestamp.valueOf((LocalDateTime) field);
                    break;
                default:
                    rowData[i] = field;
            }
        }
        buffer.add(rowData);
    }

    /**
     * Writes one buffered batch and closes its driver-owned bulk-copy resource.
     *
     * <p>The batch is discarded only after both operations succeed, preserving the retry contract.
     */
    @Override
    public void executeBatch() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        try (SqlserverBulkCopyProxy bulkCopy = new SqlserverBulkCopyProxy(connection)) {
            bulkCopy.setDestinationTableName(schemaTableName);
            for (int i = 0; i < columns.size(); i++) {
                bulkCopy.addColumnMapping(i + 1, columns.get(i).getName());
            }
            bulkCopy.configure(buffer.size());
            bulkCopy.writeToServer(new MemoryBulkData(metadata, buffer));
        }
        // Retain the batch if writing or closing fails so the JDBC owner can retry it.
        buffer.clear();
    }

    @Override
    public void clearBatch() {
        buffer.clear();
    }

    @Override
    public void closeStatements() {
        // JdbcOutputFormat also calls this before reconnecting the same executor. Keep the
        // pending batch for replay, and never execute data merely because statements are closing.
    }

    /**
     * Copies selected destination metadata before closing the discovery statement and result set.
     *
     * <p>Precision and scale remain destination-owned; TIMESTAMP_TZ uses the JDBC 4.2 type matching
     * SeaTunnel's OffsetDateTime values instead of the vendor-specific value-object type.
     */
    private List<ColumnMetadata> readColumnMetadata() throws SQLException {
        String selectedColumns =
                columns.stream()
                        .map(column -> quoteIdentifier(column.getName()))
                        .collect(Collectors.joining(", "));
        String query = "SELECT " + selectedColumns + " FROM " + schemaTableName + " WHERE 1=0";
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(query)) {
            ResultSetMetaData sourceMetadata = result.getMetaData();
            List<ColumnMetadata> snapshots = new ArrayList<>(columns.size());
            for (int index = 1; index <= columns.size(); index++) {
                snapshots.add(
                        new ColumnMetadata(
                                sourceMetadata.getColumnName(index),
                                // The vendor DATETIMEOFFSET code requires a vendor value object.
                                // SeaTunnel supplies OffsetDateTime, whose JDBC 4.2 code is 2014.
                                columns.get(index - 1).getDataType().getSqlType()
                                                == SqlType.TIMESTAMP_TZ
                                        ? Types.TIMESTAMP_WITH_TIMEZONE
                                        : sourceMetadata.getColumnType(index),
                                sourceMetadata.getPrecision(index),
                                sourceMetadata.getScale(index)));
            }
            return snapshots;
        }
    }

    private static String quoteIdentifier(String name) {
        return "[" + name.replace("]", "]]") + "]";
    }

    /**
     * Stores destination metadata after the discovery query has closed its JDBC resources.
     *
     * <p>Some drivers invalidate metadata when its result set is closed.
     */
    private static final class ColumnMetadata {
        private final String name;
        private final int type;
        private final int precision;
        private final int scale;

        private ColumnMetadata(String name, int type, int precision, int scale) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    /**
     * Supplies the selected input columns in source order to the JDBC driver.
     *
     * <p>Explicit column mappings handle destination columns omitted from the incoming row.
     */
    static class MemoryBulkData implements SqlserverBulkCopyProxy.BulkData {
        private final List<ColumnMetadata> metadata;
        private final Iterator<Object[]> iterator;
        private Object[] current;

        MemoryBulkData(List<ColumnMetadata> metadata, List<Object[]> rows) {
            this.metadata = metadata;
            this.iterator = rows.iterator();
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            Set<Integer> ordinals = new LinkedHashSet<>();
            for (int index = 1; index <= metadata.size(); index++) {
                ordinals.add(index);
            }
            return ordinals;
        }

        @Override
        public Object[] getRowData() {
            return current;
        }

        @Override
        public boolean next() {
            if (!iterator.hasNext()) {
                return false;
            }
            current = iterator.next();
            return true;
        }

        @Override
        public String getColumnName(int column) {
            return metadata.get(column - 1).name;
        }

        @Override
        public int getColumnType(int column) {
            return metadata.get(column - 1).type;
        }

        @Override
        public int getPrecision(int column) {
            return metadata.get(column - 1).precision;
        }

        @Override
        public int getScale(int column) {
            return metadata.get(column - 1).scale;
        }
    }
}

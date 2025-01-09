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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Function;

public class DynamicBufferedBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {

    private final CopyBatchStatementExecutor copyBatchStatementExecutor;

    private CopyBatchStatementExecutor tmpCopyBatchStatementExecutor;

    private final JdbcBatchStatementExecutor<SeaTunnelRow> bufferReducedBatchStatementExecutor;

    private final TablePath tablePath;
    private TablePath tmpTablePath;
    private final TableSchema tableSchema;
    private final JdbcDialect dialect;
    private final JdbcSinkConfig jdbcSinkConfig;
    private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;

    private Connection connection;

    private Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    private boolean hasUpsertRowKind = false;

    private String dialectOrCompatibleMode;

    private final LinkedHashMap<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> buffer =
            new LinkedHashMap<>();

    public DynamicBufferedBatchStatementExecutor(
            TablePath tablePath,
            TableSchema tableSchema,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcBatchStatementExecutor<SeaTunnelRow> bufferReducedBatchStatementExecutor,
            Function<SeaTunnelRow, SeaTunnelRow> valueTransform) {
        this.tablePath = tablePath;
        this.tableSchema = tableSchema;
        this.dialect = dialect;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.bufferReducedBatchStatementExecutor = bufferReducedBatchStatementExecutor;
        this.valueTransform = valueTransform;

        if (jdbcSinkConfig.getJdbcConnectionConfig() != null
                && StringUtils.isNotBlank(
                        jdbcSinkConfig.getJdbcConnectionConfig().getCompatibleMode())) {
            dialectOrCompatibleMode = jdbcSinkConfig.getJdbcConnectionConfig().getCompatibleMode();
        } else {
            dialectOrCompatibleMode = dialect.dialectName();
        }
        this.copyBatchStatementExecutor =
                CopyBatchStatementExecutorFactory.create(dialectOrCompatibleMode);
        this.copyBatchStatementExecutor.init(tablePath, tableSchema);

        if (jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.MERGE)
                || jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.COPY_MERGE)) {
            if (jdbcSinkConfig.getPrimaryKeys() == null
                    || jdbcSinkConfig.getPrimaryKeys().isEmpty()) {
                throw new RuntimeException(
                        "Primary key is not set, can not execute merge operation");
            }
            if (StringUtils.isNotBlank(jdbcSinkConfig.getTempTableName())) {
                this.tmpTablePath =
                        TablePath.of(
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                jdbcSinkConfig.getTempTableName().toLowerCase());
            } else {
                this.tmpTablePath =
                        TablePath.of(
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName() + "_tmp");
            }
            this.tmpCopyBatchStatementExecutor =
                    createTmpCopyBufferedExecutor(tmpTablePath, tableSchema);

            List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
            String[] pkNames = primaryKeys.toArray(new String[0]);
            int[] pkFields =
                    Arrays.stream(pkNames)
                            .mapToInt(tableSchema.toPhysicalRowDataType()::indexOf)
                            .toArray();
            this.keyExtractor = JdbcOutputFormatBuilder.createKeyExtractor(pkFields);
        }
    }

    private CopyBatchStatementExecutor createTmpCopyBufferedExecutor(
            TablePath tmpTablePath, TableSchema tableSchema) {
        List<Column> tmpColumnsSchema = new ArrayList<>(tableSchema.getColumns());
        tmpColumnsSchema.add(
                new PhysicalColumn(
                        jdbcSinkConfig.getTempColumnBatchCode(),
                        BasicType.STRING_TYPE,
                        null,
                        null,
                        false,
                        null,
                        null));
        tmpColumnsSchema.add(
                new PhysicalColumn(
                        jdbcSinkConfig.getTempColumnRowKind(),
                        BasicType.INT_TYPE,
                        null,
                        null,
                        false,
                        null,
                        null));
        TableSchema tmpTableSchema =
                new TableSchema(
                        tmpColumnsSchema,
                        tableSchema.getPrimaryKey(),
                        tableSchema.getConstraintKeys());
        CopyBatchStatementExecutor copyBatchStatementExecutor =
                CopyBatchStatementExecutorFactory.create(dialectOrCompatibleMode);
        copyBatchStatementExecutor.init(tmpTablePath, tmpTableSchema);
        return copyBatchStatementExecutor;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
        this.copyBatchStatementExecutor.prepareStatements(connection);
        if (this.tmpCopyBatchStatementExecutor != null) {
            this.tmpCopyBatchStatementExecutor.prepareStatements(connection);
        }
        if (this.bufferReducedBatchStatementExecutor != null) {
            this.bufferReducedBatchStatementExecutor.prepareStatements(connection);
        }
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        if (jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.COPY)
                || jdbcSinkConfig.isUseCopyStatement()) {
            if (!RowKind.INSERT.equals(record.getRowKind())) {
                throw new RuntimeException("Only support INSERT row kind when writeMode is COPY");
            }
            copyBatchStatementExecutor.addToBatch(record);
        } else {
            if (RowKind.UPDATE_BEFORE.equals(record.getRowKind())) {
                hasUpsertRowKind = true;
                return;
            }
            if ((jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.COPY_MERGE)
                            || jdbcSinkConfig
                                    .getWriteMode()
                                    .equals(JdbcSinkConfig.WriteMode.COPY_SQL))
                    && !hasUpsertRowKind
                    && RowKind.INSERT.equals(record.getRowKind())) {
                copyBatchStatementExecutor.addToBatch(record);
                return;
            }
            if (!RowKind.INSERT.equals(record.getRowKind())) {
                hasUpsertRowKind = true;
            }
            if (bufferReducedBatchStatementExecutor != null) {
                bufferReducedBatchStatementExecutor.addToBatch(record);
                return;
            }
            // reduce row to buffer
            if (keyExtractor == null) {
                throw new RuntimeException(
                        "Primary key is not set, can not execute merge operation");
            }
            SeaTunnelRow key = keyExtractor.apply(record);
            boolean changeFlag =
                    RowKind.INSERT.equals(record.getRowKind())
                            || RowKind.UPDATE_AFTER.equals(record.getRowKind());
            SeaTunnelRow value = valueTransform.apply(record);
            buffer.put(key, Pair.of(changeFlag, value));
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        if (jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.COPY)
                || jdbcSinkConfig.isUseCopyStatement()) {
            copyBatchStatementExecutor.executeBatch();
            return;
        }
        if ((jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.COPY_MERGE)
                        || jdbcSinkConfig.getWriteMode().equals(JdbcSinkConfig.WriteMode.COPY_SQL))
                && !copyBatchStatementExecutor.isFlushed()) {
            // handle batch first
            copyBatchStatementExecutor.executeBatch();
        }

        if (bufferReducedBatchStatementExecutor != null) {
            bufferReducedBatchStatementExecutor.executeBatch();
            return;
        }

        boolean originAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            String batchCode = UUID.randomUUID().toString().replace("-", "");
            // copy to tmp table
            for (Pair<Boolean, SeaTunnelRow> pariRow : buffer.values()) {
                SeaTunnelRow row = pariRow.getRight();
                Object[] fields = row.getFields();
                Object[] newFields = new Object[fields.length + 2];
                System.arraycopy(fields, 0, newFields, 0, fields.length);
                newFields[newFields.length - 2] = batchCode;
                newFields[newFields.length - 1] = (int) row.getRowKind().toByteValue();
                SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                tmpCopyBatchStatementExecutor.addToBatch(newRow);
            }
            tmpCopyBatchStatementExecutor.executeBatch();

            mergeTempTableToTargetTable(batchCode);

            connection.commit();
            buffer.clear();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originAutoCommit);
        }
    }

    private void mergeTempTableToTargetTable(String batchCode) throws SQLException {
        // handle buffer for merge
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        List<String> primaryKeyColumns = primaryKey.getColumnNames();
        StringJoiner condition = new StringJoiner(" AND ");
        for (String primaryKeyColumn : primaryKeyColumns) {
            condition.add(primaryKeyColumn + "=tmp." + primaryKeyColumn);
        }
        String table = dialect.extractTableName(tablePath);
        String tmpTable = dialect.extractTableName(tmpTablePath);
        String deleteSQL =
                String.format(
                        "DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s tmp WHERE tmp.%s=? AND tmp.%s=? AND %s)",
                        dialect.tableIdentifier(jdbcSinkConfig.getDatabase(), table),
                        dialect.tableIdentifier(jdbcSinkConfig.getDatabase(), tmpTable),
                        jdbcSinkConfig.getTempColumnBatchCode(),
                        jdbcSinkConfig.getTempColumnRowKind(),
                        condition);
        try (PreparedStatement pStmt = connection.prepareStatement(deleteSQL)) {
            pStmt.setString(1, batchCode);
            pStmt.setInt(2, RowKind.DELETE.toByteValue());
            pStmt.executeUpdate();
        }

        // handle upsert row kind
        final List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
        StringJoiner baseColumns = new StringJoiner(", ");
        for (String fieldName : tableSchema.getFieldNames()) {
            baseColumns.add(dialect.quoteIdentifier(fieldName));
        }
        String sourceSQL =
                String.format(
                        "SELECT %s FROM %s WHERE %s = ? AND %s != ? ",
                        baseColumns,
                        dialect.tableIdentifier(jdbcSinkConfig.getDatabase(), tmpTable),
                        jdbcSinkConfig.getTempColumnBatchCode(),
                        jdbcSinkConfig.getTempColumnRowKind());
        Optional<String> mergeStatement =
                dialect.getMergeStatement(
                        sourceSQL,
                        jdbcSinkConfig.getDatabase(),
                        table,
                        tableSchema.getFieldNames(),
                        primaryKeys.toArray(new String[0]),
                        jdbcSinkConfig.isPrimaryKeyUpdated());
        if (!mergeStatement.isPresent()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "unsupported merge statement for dialect: %s", dialect.dialectName()));
        }
        try (PreparedStatement pStmt = connection.prepareStatement(mergeStatement.get())) {
            pStmt.setString(1, batchCode);
            pStmt.setInt(2, RowKind.DELETE.toByteValue());
            pStmt.executeUpdate();
        }

        // delete temp date from tmp table
        String deleteTmpSQL =
                String.format(
                        "DELETE FROM %s WHERE %s=?",
                        dialect.tableIdentifier(jdbcSinkConfig.getDatabase(), tmpTable),
                        jdbcSinkConfig.getTempColumnBatchCode());
        try (PreparedStatement pStmt = connection.prepareStatement(deleteTmpSQL)) {
            pStmt.setString(1, batchCode);
            pStmt.executeUpdate();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatch();
        }
        copyBatchStatementExecutor.closeStatements();
        if (tmpCopyBatchStatementExecutor != null) {
            tmpCopyBatchStatementExecutor.closeStatements();
        }
        if (bufferReducedBatchStatementExecutor != null) {
            bufferReducedBatchStatementExecutor.closeStatements();
        }
    }
}

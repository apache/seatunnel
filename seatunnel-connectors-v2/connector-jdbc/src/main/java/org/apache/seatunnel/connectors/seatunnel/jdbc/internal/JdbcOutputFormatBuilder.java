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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.sink.SinkMetricsCalc;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferReducedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.CopyManagerBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.FieldNamedPreparedStatement;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.InsertOrUpdateBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.SimpleBatchStatementExecutor;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class JdbcOutputFormatBuilder {
    @NonNull private final JdbcDialect dialect;
    @NonNull private final JdbcConnectionProvider connectionProvider;
    @NonNull private final JdbcSinkConfig jdbcSinkConfig;
    @NonNull private final TableSchema tableSchema;
    @NonNull private final SinkMetricsCalc sinkMetricsCalc;

    public JdbcOutputFormat build() {
        JdbcOutputFormat.StatementExecutorFactory statementExecutorFactory;

        final String database = jdbcSinkConfig.getDatabase();
        final String table =
                dialect.extractTableName(
                        TablePath.of(
                                jdbcSinkConfig.getDatabase() + "." + jdbcSinkConfig.getTable()));

        final List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
        if (jdbcSinkConfig.isUseCopyStatement()) {
            statementExecutorFactory =
                    () ->
                            createCopyInBufferStatementExecutor(
                                    createCopyInBatchStatementExecutor(
                                            dialect, table, tableSchema));
        } else if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getSimpleSql(),
                                    tableSchema,
                                    dialect.getRowConverter());
        } else if (primaryKeys == null || primaryKeys.isEmpty()) {
            statementExecutorFactory =
                    () -> createSimpleBufferedExecutor(dialect, database, table, tableSchema);
        } else {
            statementExecutorFactory =
                    () ->
                            createUpsertBufferedExecutor(
                                    dialect,
                                    database,
                                    table,
                                    tableSchema,
                                    primaryKeys.toArray(new String[0]),
                                    jdbcSinkConfig.isEnableUpsert(),
                                    jdbcSinkConfig.isPrimaryKeyUpdated(),
                                    jdbcSinkConfig.isSupportUpsertByInsertOnly());
        }

        return new JdbcOutputFormat(
                connectionProvider,
                jdbcSinkConfig.getJdbcConnectionConfig(),
                statementExecutorFactory,
                sinkMetricsCalc);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(
            JdbcDialect dialect, String database, String table, TableSchema tableSchema) {
        String insertSQL =
                dialect.getInsertIntoStatement(database, table, tableSchema.getFieldNames());
        return createSimpleBufferedExecutor(insertSQL, tableSchema, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(
            String sql, TableSchema tableSchema, JdbcRowConverter rowConverter) {
        JdbcBatchStatementExecutor<SeaTunnelRow> simpleRowExecutor =
                createSimpleExecutor(sql, tableSchema, rowConverter);
        return new BufferedBatchStatementExecutor(simpleRowExecutor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertBufferedExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            String[] pkNames,
            boolean enableUpsert,
            boolean isPrimaryKeyUpdated,
            boolean supportUpsertByInsertOnly) {
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(tableSchema.toPhysicalRowDataType()::indexOf)
                        .toArray();

        TableSchema pkSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.stream(pkFields)
                                        .mapToObj(
                                                (IntFunction<Column>) tableSchema.getColumns()::get)
                                        .collect(Collectors.toList()))
                        .build();

        Function<SeaTunnelRow, SeaTunnelRow> keyExtractor = createKeyExtractor(pkFields);
        JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor =
                createDeleteExecutor(dialect, database, table, pkNames, pkSchema);
        JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor =
                createUpsertExecutor(
                        dialect,
                        database,
                        table,
                        tableSchema,
                        pkNames,
                        pkSchema,
                        keyExtractor,
                        enableUpsert,
                        isPrimaryKeyUpdated,
                        supportUpsertByInsertOnly);
        return new BufferReducedBatchStatementExecutor(
                upsertExecutor, deleteExecutor, keyExtractor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            String[] pkNames,
            TableSchema pkTableSchema,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            boolean enableUpsert,
            boolean isPrimaryKeyUpdated,
            boolean supportUpsertByInsertOnly) {
        if (supportUpsertByInsertOnly) {
            return createInsertOnlyExecutor(dialect, database, table, tableSchema);
        }
        if (enableUpsert) {
            Optional<String> upsertSQL =
                    dialect.getUpsertStatement(
                            database, table, tableSchema.getFieldNames(), pkNames);
            if (upsertSQL.isPresent()) {
                return createSimpleExecutor(
                        upsertSQL.get(), tableSchema, dialect.getRowConverter());
            }
            return createInsertOrUpdateByQueryExecutor(
                    dialect,
                    database,
                    table,
                    tableSchema,
                    pkNames,
                    pkTableSchema,
                    keyExtractor,
                    isPrimaryKeyUpdated);
        }
        return createInsertOrUpdateExecutor(
                dialect, database, table, tableSchema, pkNames, isPrimaryKeyUpdated);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createCopyInBufferStatementExecutor(
            CopyManagerBatchStatementExecutor copyManagerBatchStatementExecutor) {
        return new BufferedBatchStatementExecutor(
                copyManagerBatchStatementExecutor, Function.identity());
    }

    private static CopyManagerBatchStatementExecutor createCopyInBatchStatementExecutor(
            JdbcDialect dialect, String table, TableSchema tableSchema) {
        String columns =
                Arrays.stream(tableSchema.getFieldNames())
                        .map(dialect::quoteIdentifier)
                        .collect(Collectors.joining(",", "(", ")"));
        String copyInSql = String.format("COPY %s %s FROM STDIN WITH CSV", table, columns);
        return new CopyManagerBatchStatementExecutor(copyInSql, tableSchema);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOnlyExecutor(
            JdbcDialect dialect, String database, String table, TableSchema tableSchema) {

        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, tableSchema.getFieldNames()),
                                tableSchema.getFieldNames()),
                tableSchema,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            String[] pkNames,
            boolean isPrimaryKeyUpdated) {

        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, tableSchema.getFieldNames()),
                                tableSchema.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getUpdateStatement(
                                        database,
                                        table,
                                        tableSchema.getFieldNames(),
                                        pkNames,
                                        isPrimaryKeyUpdated),
                                tableSchema.getFieldNames()),
                tableSchema,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateByQueryExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            String[] pkNames,
            TableSchema pkTableSchema,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            boolean isPrimaryKeyUpdated) {
        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getRowExistsStatement(database, table, pkNames),
                                pkNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, tableSchema.getFieldNames()),
                                tableSchema.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getUpdateStatement(
                                        database,
                                        table,
                                        tableSchema.getFieldNames(),
                                        pkNames,
                                        isPrimaryKeyUpdated),
                                tableSchema.getFieldNames()),
                pkTableSchema,
                keyExtractor,
                tableSchema,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createDeleteExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            String[] pkNames,
            TableSchema pkTableSchema) {
        String deleteSQL = dialect.getDeleteStatement(database, table, pkNames);
        return createSimpleExecutor(deleteSQL, pkTableSchema, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(
            String sql, TableSchema tableSchema, JdbcRowConverter rowConverter) {
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, sql, tableSchema.getFieldNames()),
                tableSchema,
                rowConverter);
    }

    static Function<SeaTunnelRow, SeaTunnelRow> createKeyExtractor(int[] pkFields) {
        return row -> {
            Object[] fields = new Object[pkFields.length];
            for (int i = 0; i < pkFields.length; i++) {
                fields[i] = row.getField(pkFields[i]);
            }
            SeaTunnelRow newRow = new SeaTunnelRow(fields);
            newRow.setTableId(row.getTableId());
            return newRow;
        };
    }
}

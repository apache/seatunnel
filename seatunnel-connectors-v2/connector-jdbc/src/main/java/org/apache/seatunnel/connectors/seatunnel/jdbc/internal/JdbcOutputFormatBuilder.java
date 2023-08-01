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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferReducedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferedBatchStatementExecutor;
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

@Slf4j
@RequiredArgsConstructor
public class JdbcOutputFormatBuilder {
    @NonNull private final JdbcDialect dialect;
    @NonNull private final JdbcConnectionProvider connectionProvider;
    @NonNull private final JdbcSinkConfig jdbcSinkConfig;
    @NonNull private final SeaTunnelRowType seaTunnelRowType;

    public JdbcOutputFormat build() {
        JdbcOutputFormat.StatementExecutorFactory statementExecutorFactory;

        final String database = jdbcSinkConfig.getDatabase();
        final String table =
                dialect.extractTableName(
                        TablePath.of(
                                jdbcSinkConfig.getDatabase() + "." + jdbcSinkConfig.getTable()));

        final List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
        if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getSimpleSql(),
                                    seaTunnelRowType,
                                    dialect.getRowConverter());
        } else if (primaryKeys == null || primaryKeys.isEmpty()) {
            statementExecutorFactory =
                    () -> createSimpleBufferedExecutor(dialect, database, table, seaTunnelRowType);
        } else {
            statementExecutorFactory =
                    () ->
                            createUpsertBufferedExecutor(
                                    dialect,
                                    database,
                                    table,
                                    seaTunnelRowType,
                                    primaryKeys.toArray(new String[0]),
                                    jdbcSinkConfig.isEnableUpsert(),
                                    jdbcSinkConfig.isPrimaryKeyUpdated(),
                                    jdbcSinkConfig.isSupportUpsertByInsertOnly());
        }

        return new JdbcOutputFormat(
                connectionProvider,
                jdbcSinkConfig.getJdbcConnectionConfig(),
                statementExecutorFactory);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(
            JdbcDialect dialect, String database, String table, SeaTunnelRowType rowType) {
        String insertSQL = dialect.getInsertIntoStatement(database, table, rowType.getFieldNames());
        return createSimpleBufferedExecutor(insertSQL, rowType, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(
            String sql, SeaTunnelRowType rowType, JdbcRowConverter rowConverter) {
        JdbcBatchStatementExecutor<SeaTunnelRow> simpleRowExecutor =
                createSimpleExecutor(sql, rowType, rowConverter);
        return new BufferedBatchStatementExecutor(simpleRowExecutor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertBufferedExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            SeaTunnelRowType rowType,
            String[] pkNames,
            boolean enableUpsert,
            boolean isPrimaryKeyUpdated,
            boolean supportUpsertByInsertOnly) {
        int[] pkFields = Arrays.stream(pkNames).mapToInt(rowType::indexOf).toArray();
        SeaTunnelDataType[] pkTypes =
                Arrays.stream(pkFields)
                        .mapToObj((IntFunction<SeaTunnelDataType>) rowType::getFieldType)
                        .toArray(SeaTunnelDataType[]::new);

        Function<SeaTunnelRow, SeaTunnelRow> keyExtractor = createKeyExtractor(pkFields);
        JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor =
                createDeleteExecutor(dialect, database, table, pkNames, pkTypes);
        JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor =
                createUpsertExecutor(
                        dialect,
                        database,
                        table,
                        rowType,
                        pkNames,
                        pkTypes,
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
            SeaTunnelRowType rowType,
            String[] pkNames,
            SeaTunnelDataType[] pkTypes,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            boolean enableUpsert,
            boolean isPrimaryKeyUpdated,
            boolean supportUpsertByInsertOnly) {
        if (supportUpsertByInsertOnly) {
            return createInsertOnlyExecutor(dialect, database, table, rowType);
        }
        if (enableUpsert) {
            Optional<String> upsertSQL =
                    dialect.getUpsertStatement(database, table, rowType.getFieldNames(), pkNames);
            if (upsertSQL.isPresent()) {
                return createSimpleExecutor(upsertSQL.get(), rowType, dialect.getRowConverter());
            }
            return createInsertOrUpdateByQueryExecutor(
                    dialect,
                    database,
                    table,
                    rowType,
                    pkNames,
                    pkTypes,
                    keyExtractor,
                    isPrimaryKeyUpdated);
        }
        return createInsertOrUpdateExecutor(
                dialect, database, table, rowType, pkNames, isPrimaryKeyUpdated);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOnlyExecutor(
            JdbcDialect dialect, String database, String table, SeaTunnelRowType rowType) {

        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, rowType.getFieldNames()),
                                rowType.getFieldNames()),
                rowType,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            SeaTunnelRowType rowType,
            String[] pkNames,
            boolean isPrimaryKeyUpdated) {

        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, rowType.getFieldNames()),
                                rowType.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getUpdateStatement(
                                        database,
                                        table,
                                        rowType.getFieldNames(),
                                        pkNames,
                                        isPrimaryKeyUpdated),
                                rowType.getFieldNames()),
                rowType,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateByQueryExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            SeaTunnelRowType rowType,
            String[] pkNames,
            SeaTunnelDataType[] pkTypes,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            boolean isPrimaryKeyUpdated) {
        SeaTunnelRowType keyRowType = new SeaTunnelRowType(pkNames, pkTypes);
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
                                        database, table, rowType.getFieldNames()),
                                rowType.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getUpdateStatement(
                                        database,
                                        table,
                                        rowType.getFieldNames(),
                                        pkNames,
                                        isPrimaryKeyUpdated),
                                rowType.getFieldNames()),
                keyRowType,
                keyExtractor,
                rowType,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createDeleteExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            String[] pkNames,
            SeaTunnelDataType[] pkTypes) {
        String deleteSQL = dialect.getDeleteStatement(database, table, pkNames);
        return createSimpleExecutor(deleteSQL, pkNames, pkTypes, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(
            String sql,
            String[] fieldNames,
            SeaTunnelDataType[] fieldTypes,
            JdbcRowConverter rowConverter) {
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        return createSimpleExecutor(sql, rowType, rowConverter);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(
            String sql, SeaTunnelRowType rowType, JdbcRowConverter rowConverter) {
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, sql, rowType.getFieldNames()),
                rowType,
                rowConverter);
    }

    private static Function<SeaTunnelRow, SeaTunnelRow> createKeyExtractor(int[] pkFields) {
        return row -> {
            Object[] fields = new Object[pkFields.length];
            for (int i = 0; i < pkFields.length; i++) {
                fields[i] = row.getField(pkFields[i]);
            }
            SeaTunnelRow newRow = new SeaTunnelRow(fields);
            newRow.setTableId(row.getTableId());
            newRow.setRowKind(row.getRowKind());
            return newRow;
        };
    }
}

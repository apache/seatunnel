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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferReducedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.InsertOrUpdateBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.SimpleBatchStatementExecutor;

import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;

@RequiredArgsConstructor
public class JdbcOutputFormatBuilder {
    @NonNull
    private final JdbcDialect dialect;
    @NonNull
    private final JdbcConnectionProvider connectionProvider;
    @NonNull
    private final JdbcSinkOptions jdbcSinkOptions;
    @NonNull
    private final SeaTunnelRowType seaTunnelRowType;

    public JdbcOutputFormat build() {
        JdbcOutputFormat.StatementExecutorFactory statementExecutorFactory;

        final String table = jdbcSinkOptions.getTable();
        final List<String> primaryKeys = jdbcSinkOptions.getPrimaryKeys();
        if (Strings.isNullOrEmpty(table)) {
            statementExecutorFactory = () -> createSimpleBufferedExecutor(
                jdbcSinkOptions.getSimpleSQL(), seaTunnelRowType, dialect.getRowConverter());
        } else if (primaryKeys == null || primaryKeys.isEmpty()) {
            statementExecutorFactory = () -> createSimpleBufferedExecutor(
                dialect, table, seaTunnelRowType);
        } else {
            statementExecutorFactory = () -> createUpsertBufferedExecutor(
                dialect, table, seaTunnelRowType, primaryKeys.toArray(new String[0]));
        }

        return new JdbcOutputFormat(connectionProvider,
            jdbcSinkOptions.getJdbcConnectionOptions(), statementExecutorFactory);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(JdbcDialect dialect,
                                                                                         String table,
                                                                                         SeaTunnelRowType rowType) {
        String insertSQL = dialect.getInsertIntoStatement(table, rowType.getFieldNames());
        return createSimpleBufferedExecutor(insertSQL, rowType, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(String sql,
                                                                                         SeaTunnelRowType rowType,
                                                                                         JdbcRowConverter rowConverter) {
        JdbcBatchStatementExecutor<SeaTunnelRow> simpleRowExecutor =
            createSimpleExecutor(sql, rowType, rowConverter);
        return new BufferedBatchStatementExecutor(simpleRowExecutor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertBufferedExecutor(JdbcDialect dialect,
                                                                                         String table,
                                                                                         SeaTunnelRowType rowType,
                                                                                         String[] pkNames) {
        int[] pkFields = Arrays.stream(pkNames)
            .mapToInt(Arrays.asList(rowType.getFieldNames())::indexOf)
            .toArray();
        SeaTunnelDataType[] pkTypes = Arrays.stream(pkFields)
            .mapToObj((IntFunction<SeaTunnelDataType>) index -> rowType.getFieldType(index))
            .toArray(length -> new SeaTunnelDataType[length]);

        Function<SeaTunnelRow, SeaTunnelRow> keyExtractor = createKeyExtractor(pkFields);
        JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor = createDeleteExecutor(
            dialect, table, pkNames, pkTypes);
        JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor = createUpsertExecutor(
            dialect, table, rowType, pkNames, pkTypes, keyExtractor);
        return new BufferReducedBatchStatementExecutor(
            upsertExecutor, deleteExecutor, keyExtractor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertExecutor(JdbcDialect dialect,
                                                                                 String table,
                                                                                 SeaTunnelRowType rowType,
                                                                                 String[] pkNames,
                                                                                 SeaTunnelDataType[] pkTypes,
                                                                                 Function<SeaTunnelRow, SeaTunnelRow> keyExtractor) {
        return dialect.getUpsertStatement(table, rowType.getFieldNames(), pkNames)
            .map(upsertSQL -> createSimpleExecutor(upsertSQL, rowType, dialect.getRowConverter()))
            .orElseGet(() -> createInsertOrUpdateExecutor(dialect, table, rowType, pkNames, pkTypes, keyExtractor));
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateExecutor(JdbcDialect dialect,
                                                                                         String table,
                                                                                         SeaTunnelRowType rowType,
                                                                                         String[] pkNames,
                                                                                         SeaTunnelDataType[] pkTypes,
                                                                                         Function<SeaTunnelRow, SeaTunnelRow> keyExtractor) {

        SeaTunnelRowType keyRowType = new SeaTunnelRowType(pkNames, pkTypes);
        return new InsertOrUpdateBatchStatementExecutor(
            connection -> connection.prepareStatement(dialect.getRowExistsStatement(table, pkNames)),
            connection -> connection.prepareStatement(dialect.getInsertIntoStatement(table, rowType.getFieldNames())),
            connection -> connection.prepareStatement(dialect.getUpdateStatement(table, rowType.getFieldNames(), pkNames)),
            rowType,
            keyRowType,
            keyExtractor,
            dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createDeleteExecutor(JdbcDialect dialect,
                                                                                 String table,
                                                                                 String[] pkNames,
                                                                                 SeaTunnelDataType[] pkTypes) {
        String deleteSQL = dialect.getDeleteStatement(table, pkNames);
        return createSimpleExecutor(deleteSQL, pkNames, pkTypes, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(String sql,
                                                                                 String[] fieldNames,
                                                                                 SeaTunnelDataType[] fieldTypes,
                                                                                 JdbcRowConverter rowConverter) {
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        return createSimpleExecutor(sql, rowType, rowConverter);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(String sql,
                                                                                 SeaTunnelRowType rowType,
                                                                                 JdbcRowConverter rowConverter) {
        return new SimpleBatchStatementExecutor(
            connection -> connection.prepareStatement(sql),
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
            return row;
        };
    }
}

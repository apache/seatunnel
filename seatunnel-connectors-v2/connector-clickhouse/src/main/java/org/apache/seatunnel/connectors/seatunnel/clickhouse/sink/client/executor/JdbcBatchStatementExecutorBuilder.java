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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;

@Setter
@Accessors(chain = true)
public class JdbcBatchStatementExecutorBuilder {
    private static final String MERGE_TREE_ENGINE_SUFFIX = "MergeTree";
    private static final String REPLACING_MERGE_TREE_ENGINE_SUFFIX = "ReplacingMergeTree";
    private String table;
    private String tableEngine;
    private SeaTunnelRowType rowType;
    private String[] primaryKeys;
    private Map<String, String> clickhouseTableSchema;
    private boolean supportUpsert;
    private boolean allowExperimentalLightweightDelete;
    private boolean clickhouseServerEnableExperimentalLightweightDelete;
    private String[] orderByKeys;

    private boolean supportMergeTreeEngineExperimentalLightweightDelete() {
        return tableEngine.endsWith(MERGE_TREE_ENGINE_SUFFIX) && allowExperimentalLightweightDelete;
    }

    private boolean supportReplacingMergeTreeTableUpsert() {
        return tableEngine.endsWith(REPLACING_MERGE_TREE_ENGINE_SUFFIX)
                && Arrays.equals(primaryKeys, orderByKeys);
    }

    private String[] getDefaultProjectionFields() {
        List<String> fieldNames = Arrays.asList(rowType.getFieldNames());
        return fieldNames.stream()
                .filter(clickhouseTableSchema::containsKey)
                .toArray(String[]::new);
    }

    public JdbcBatchStatementExecutor build() {
        Objects.requireNonNull(table);
        Objects.requireNonNull(tableEngine);
        Objects.requireNonNull(rowType);
        Objects.requireNonNull(clickhouseTableSchema);

        JdbcRowConverter valueRowConverter =
                new JdbcRowConverter(rowType, clickhouseTableSchema, getDefaultProjectionFields());
        if (primaryKeys == null || primaryKeys.length == 0) {
            // INSERT: writer all events when primary-keys is empty
            return createInsertBufferedExecutor(table, rowType, valueRowConverter);
        }

        int[] pkFields =
                Arrays.stream(primaryKeys)
                        .mapToInt(Arrays.asList(rowType.getFieldNames())::indexOf)
                        .toArray();
        SeaTunnelDataType[] pkTypes = getKeyTypes(pkFields, rowType);
        JdbcRowConverter pkRowConverter =
                new JdbcRowConverter(
                        new SeaTunnelRowType(primaryKeys, pkTypes),
                        clickhouseTableSchema,
                        primaryKeys);
        Function<SeaTunnelRow, SeaTunnelRow> pkExtractor = createKeyExtractor(pkFields);

        if (supportMergeTreeEngineExperimentalLightweightDelete()) {
            boolean convertUpdateBeforeEventToDeleteAction;
            // DELETE: delete sql
            JdbcBatchStatementExecutor deleteExecutor =
                    createDeleteExecutor(
                            table,
                            primaryKeys,
                            pkRowConverter,
                            !clickhouseServerEnableExperimentalLightweightDelete);
            JdbcBatchStatementExecutor updateExecutor;
            if (supportReplacingMergeTreeTableUpsert()) {
                // ReplacingMergeTree Update Row: upsert row by order-by-keys(update_after event)
                updateExecutor = createInsertExecutor(table, rowType, valueRowConverter);
                convertUpdateBeforeEventToDeleteAction = false;
            } else {
                // *MergeTree Update Row:
                // 1. delete(update_before event) + insert or update by query
                // primary-keys(update_after event)
                // 2. delete(update_before event) + insert(update_after event)
                updateExecutor =
                        supportUpsert
                                ? createUpsertExecutor(
                                        table,
                                        rowType,
                                        primaryKeys,
                                        pkExtractor,
                                        pkRowConverter,
                                        valueRowConverter)
                                : createInsertExecutor(table, rowType, valueRowConverter);
                convertUpdateBeforeEventToDeleteAction = true;
            }
            return new ReduceBufferedBatchStatementExecutor(
                    updateExecutor,
                    deleteExecutor,
                    pkExtractor,
                    Function.identity(),
                    !convertUpdateBeforeEventToDeleteAction);
        }

        // DELETE: alter table delete sql
        JdbcBatchStatementExecutor deleteExecutor =
                createAlterTableDeleteExecutor(table, primaryKeys, pkRowConverter);
        JdbcBatchStatementExecutor updateExecutor;
        if (supportReplacingMergeTreeTableUpsert()) {
            updateExecutor = createInsertExecutor(table, rowType, valueRowConverter);
        } else {
            // Other-Engine Update Row:
            // 1. insert or update by query primary-keys(insert/update_after event)
            // 2. insert(insert event) + alter table update(update_after event)
            updateExecutor =
                    supportUpsert
                            ? createUpsertExecutor(
                                    table,
                                    rowType,
                                    primaryKeys,
                                    pkExtractor,
                                    pkRowConverter,
                                    valueRowConverter)
                            : createInsertOrUpdateExecutor(
                                    table, rowType, primaryKeys, valueRowConverter);
        }
        return new ReduceBufferedBatchStatementExecutor(
                updateExecutor, deleteExecutor, pkExtractor, Function.identity(), true);
    }

    private static JdbcBatchStatementExecutor createInsertBufferedExecutor(
            String table, SeaTunnelRowType rowType, JdbcRowConverter rowConverter) {
        return new BufferedBatchStatementExecutor(
                createInsertExecutor(table, rowType, rowConverter), Function.identity());
    }

    private static JdbcBatchStatementExecutor createInsertOrUpdateExecutor(
            String table,
            SeaTunnelRowType rowType,
            String[] pkNames,
            JdbcRowConverter rowConverter) {
        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                SqlUtils.getInsertIntoStatement(table, rowType.getFieldNames()),
                                rowType.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                SqlUtils.getAlterTableUpdateStatement(
                                        table, rowType.getFieldNames(), pkNames),
                                rowType.getFieldNames()),
                rowConverter);
    }

    private static JdbcBatchStatementExecutor createUpsertExecutor(
            String table,
            SeaTunnelRowType rowType,
            String[] pkNames,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            JdbcRowConverter keyConverter,
            JdbcRowConverter valueConverter) {
        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                SqlUtils.getRowExistsStatement(table, pkNames),
                                pkNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                SqlUtils.getInsertIntoStatement(table, rowType.getFieldNames()),
                                rowType.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                SqlUtils.getAlterTableUpdateStatement(
                                        table, rowType.getFieldNames(), pkNames),
                                rowType.getFieldNames()),
                keyExtractor,
                keyConverter,
                valueConverter);
    }

    private static JdbcBatchStatementExecutor createInsertExecutor(
            String table, SeaTunnelRowType rowType, JdbcRowConverter rowConverter) {
        String insertSQL = SqlUtils.getInsertIntoStatement(table, rowType.getFieldNames());
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, insertSQL, rowType.getFieldNames()),
                rowConverter);
    }

    private static JdbcBatchStatementExecutor createDeleteExecutor(
            String table,
            String[] primaryKeys,
            JdbcRowConverter rowConverter,
            boolean enableExperimentalLightweightDelete) {
        String deleteSQL =
                SqlUtils.getDeleteStatement(
                        table, primaryKeys, enableExperimentalLightweightDelete);
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, deleteSQL, primaryKeys),
                rowConverter);
    }

    private static JdbcBatchStatementExecutor createAlterTableDeleteExecutor(
            String table, String[] primaryKeys, JdbcRowConverter rowConverter) {
        String alterTableDeleteSQL = SqlUtils.getAlterTableDeleteStatement(table, primaryKeys);
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, alterTableDeleteSQL, primaryKeys),
                rowConverter);
    }

    private static SeaTunnelDataType[] getKeyTypes(int[] pkFields, SeaTunnelRowType rowType) {
        return Arrays.stream(pkFields)
                .mapToObj((IntFunction<SeaTunnelDataType>) rowType::getFieldType)
                .toArray(SeaTunnelDataType[]::new);
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

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

package io.debezium.connector.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import oracle.jdbc.OracleTypes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The schema of an Oracle database.
 *
 * @author Gunnar Morling
 */
public class OracleDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    private final OracleDdlParser ddlParser;
    private final ConcurrentMap<TableId, List<Column>> lobColumnsByTableId =
            new ConcurrentHashMap<>();
    private final OracleValueConverters valueConverters;

    private boolean storageInitializationExecuted = false;

    public OracleDatabaseSchema(
            OracleConnectorConfig connectorConfig,
            OracleValueConverters valueConverters,
            DefaultValueConverter defaultValueConverter,
            SchemaNameAdjuster schemaNameAdjuster,
            TopicSelector<TableId> topicSelector,
            TableNameCaseSensitivity tableNameCaseSensitivity) {
        super(
                connectorConfig,
                topicSelector,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getSanitizeFieldNames(),
                        false),
                TableNameCaseSensitivity.INSENSITIVE.equals(tableNameCaseSensitivity),
                connectorConfig.getKeyMapper());

        this.valueConverters = valueConverters;
        this.ddlParser =
                new OracleDdlParser(
                        true,
                        false,
                        connectorConfig.isSchemaCommentsHistoryEnabled(),
                        valueConverters,
                        connectorConfig.getTableFilters().dataCollectionFilter());
    }

    public Tables getTables() {
        return tables();
    }

    public OracleValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public OracleDdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange
                        .getTableChanges()
                        .forEach(
                                x -> {
                                    buildAndRegisterSchema(x.getTable());
                                    tables().overwriteTable(x.getTable());
                                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (!databaseHistory.storeOnlyCapturedTables()
                || schemaChange.getTables().stream()
                        .map(Table::id)
                        .anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug(
                    "Recorded DDL statements for database '{}': {}",
                    schemaChange.getDatabase(),
                    schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        storageInitializationExecuted = true;
    }

    public boolean isStorageInitializationExecuted() {
        return storageInitializationExecuted;
    }

    /** Return true if the database history entity exists */
    public boolean historyExists() {
        return databaseHistory.exists();
    }

    @Override
    protected void removeSchema(TableId id) {
        super.removeSchema(id);
        lobColumnsByTableId.remove(id);
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        if (getTableFilter().isIncluded(table.id())) {
            super.buildAndRegisterSchema(table);

            // Cache LOB column mappings for performance
            buildAndRegisterTableLobColumns(table);
        }
    }

    /**
     * Get a list of large object (LOB) columns for the specified relational table identifier.
     *
     * @param id the relational table identifier
     * @return a list of LOB columns, may be empty if the table has no LOB columns
     */
    public List<Column> getLobColumnsForTable(TableId id) {
        return lobColumnsByTableId.getOrDefault(id, Collections.emptyList());
    }

    /**
     * Returns whether the specified value is the unavailable value placeholder for an LOB column.
     */
    public boolean isColumnUnavailableValuePlaceholder(Column column, Object value) {
        if (isClobColumn(column) || isXmlColumn(column)) {
            return valueConverters.getUnavailableValuePlaceholderString().equals(value);
        } else if (isBlobColumn(column)) {
            return ByteBuffer.wrap(valueConverters.getUnavailableValuePlaceholderBinary())
                    .equals(value);
        }
        return false;
    }

    /** Return whether the provided relational column model is a LOB data type. */
    public static boolean isLobColumn(Column column) {
        return isClobColumn(column) || isBlobColumn(column);
    }

    /** Return whether the provided relational column model is an XML data type. */
    public static boolean isXmlColumn(Column column) {
        return column.jdbcType() == OracleTypes.SQLXML;
    }

    /** Returns whether the provided relational column model is a CLOB or NCLOB data type. */
    private static boolean isClobColumn(Column column) {
        return column.jdbcType() == OracleTypes.CLOB || column.jdbcType() == OracleTypes.NCLOB;
    }

    /** Returns whether the provided relational column model is a CLOB data type. */
    private static boolean isBlobColumn(Column column) {
        return column.jdbcType() == OracleTypes.BLOB;
    }

    private void buildAndRegisterTableLobColumns(Table table) {
        final List<Column> lobColumns = new ArrayList<>();
        for (Column column : table.columns()) {
            switch (column.jdbcType()) {
                case OracleTypes.CLOB:
                case OracleTypes.NCLOB:
                case OracleTypes.BLOB:
                case OracleTypes.SQLXML:
                    lobColumns.add(column);
                    break;
            }
        }
        if (!lobColumns.isEmpty()) {
            lobColumnsByTableId.put(table.id(), lobColumns);
        } else {
            lobColumnsByTableId.remove(table.id());
        }
    }
}

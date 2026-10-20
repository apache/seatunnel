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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Helper for persisting Debezium table definitions in Vitess split state.
 *
 * <p>Vitess may resume from a VGTID without replaying FIELD metadata immediately, so the connector
 * keeps a checkpoint-safe copy of the latest known Debezium table schema.
 */
public final class VitessTableSchemaState {

    private static final String SERIALIZATION_TOPIC = "vitess-table-schema-state";

    private static final ConnectTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new ConnectTableChangeSerializer();

    private static final JsonConverter JSON_CONVERTER = new JsonConverter();

    static {
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", true), false);
    }

    private VitessTableSchemaState() {}

    /** Serializes a Debezium table definition into checkpoint-safe bytes. */
    public static byte[] serialize(Table table) {
        TableChanges tableChanges = new TableChanges().create(table);
        Struct tableChangeStruct = TABLE_CHANGE_SERIALIZER.serialize(tableChanges).get(0);
        return JSON_CONVERTER.fromConnectData(
                SERIALIZATION_TOPIC, tableChangeStruct.schema(), tableChangeStruct);
    }

    /** Restores one Debezium table definition from checkpoint bytes. */
    public static Table deserialize(byte[] tableSchemaBytes) {
        if (tableSchemaBytes == null || tableSchemaBytes.length == 0) {
            return null;
        }
        Struct tableChangeStruct =
                (Struct)
                        JSON_CONVERTER.toConnectData(SERIALIZATION_TOPIC, tableSchemaBytes).value();
        TableChanges tableChanges =
                TABLE_CHANGE_SERIALIZER.deserialize(
                        Collections.singletonList(tableChangeStruct), false);
        return tableChanges.iterator().next().getTable();
    }

    /**
     * Builds the initial schema snapshot from user-declared catalog tables.
     *
     * <p>This bootstrap path keeps startup.mode=specific reproducible even before VTGate re-sends
     * FIELD metadata for the captured tables.
     */
    public static Map<String, byte[]> serializeCatalogTables(List<CatalogTable> catalogTables) {
        if (catalogTables == null || catalogTables.isEmpty()) {
            return null;
        }
        Map<String, byte[]> tableSchemas = new HashMap<>(catalogTables.size());
        for (CatalogTable catalogTable : catalogTables) {
            Table table = fromCatalogTable(catalogTable);
            tableSchemas.put(table.id().toDoubleQuotedString(), serialize(table));
        }
        return tableSchemas;
    }

    private static Table fromCatalogTable(CatalogTable catalogTable) {
        TableId tableId =
                new TableId(
                        null,
                        catalogTable.getTablePath().getDatabaseName(),
                        catalogTable.getTablePath().getTableName());
        List<io.debezium.relational.Column> columns =
                new java.util.ArrayList<>(catalogTable.getTableSchema().getColumns().size());
        int position = 1;
        for (org.apache.seatunnel.api.table.catalog.Column catalogColumn :
                catalogTable.getTableSchema().getColumns()) {
            if (!catalogColumn.isPhysical()) {
                continue;
            }
            columns.add(toDebeziumColumn(catalogColumn, position++));
        }

        io.debezium.relational.TableEditor tableEditor = Table.editor().tableId(tableId);
        tableEditor.setColumns(columns);
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null) {
            tableEditor.setPrimaryKeyNames(primaryKey.getColumnNames());
        }
        return tableEditor.create();
    }

    /**
     * Converts a SeaTunnel catalog column into a Debezium relational column definition.
     *
     * <p>The bootstrap schema is only used until real FIELD metadata arrives, so generic SQL type
     * mappings are sufficient when the catalog column does not provide a database-specific
     * sourceType.
     */
    private static io.debezium.relational.Column toDebeziumColumn(
            org.apache.seatunnel.api.table.catalog.Column catalogColumn, int position) {
        io.debezium.relational.ColumnEditor columnEditor =
                io.debezium.relational.Column.editor()
                        .name(catalogColumn.getName())
                        .jdbcType(resolveJdbcType(catalogColumn))
                        .position(position)
                        .optional(catalogColumn.isNullable())
                        .comment(catalogColumn.getComment());

        String typeExpression = normalizeTypeExpression(catalogColumn.getSourceType());
        String typeName =
                typeExpression == null
                        ? defaultTypeName(catalogColumn.getDataType().getSqlType())
                        : baseTypeName(typeExpression);
        if (typeExpression == null) {
            columnEditor.type(typeName);
        } else {
            columnEditor.type(typeName, typeExpression);
        }

        if (catalogColumn.getColumnLength() != null) {
            columnEditor.length(Math.toIntExact(catalogColumn.getColumnLength()));
        }
        if (catalogColumn.getScale() != null) {
            columnEditor.scale(catalogColumn.getScale());
        }
        return columnEditor.create();
    }

    private static int resolveJdbcType(org.apache.seatunnel.api.table.catalog.Column column) {
        switch (column.getDataType().getSqlType()) {
            case STRING:
                return Types.VARCHAR;
            case BOOLEAN:
                return Types.BOOLEAN;
            case TINYINT:
                return Types.TINYINT;
            case SMALLINT:
                return Types.SMALLINT;
            case INT:
                return Types.INTEGER;
            case BIGINT:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case BYTES:
                return Types.BINARY;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return Types.TIMESTAMP;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Vitess CDC bootstrap schema does not support catalog SQL type '%s' for column '%s'.",
                                column.getDataType().getSqlType(), column.getName()));
        }
    }

    private static String defaultTypeName(SqlType sqlType) {
        switch (sqlType) {
            case STRING:
                return "VARCHAR";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case BYTES:
                return "BLOB";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return "TIMESTAMP";
            default:
                throw new IllegalArgumentException(
                        "Unsupported Vitess catalog SQL type: " + sqlType);
        }
    }

    private static String normalizeTypeExpression(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        String trimmed = sourceType.trim();
        return trimmed.isEmpty() ? null : trimmed.toUpperCase(Locale.ROOT);
    }

    private static String baseTypeName(String typeExpression) {
        int parenthesisIndex = typeExpression.indexOf('(');
        if (parenthesisIndex < 0) {
            return typeExpression;
        }
        return typeExpression.substring(0, parenthesisIndex).trim();
    }
}

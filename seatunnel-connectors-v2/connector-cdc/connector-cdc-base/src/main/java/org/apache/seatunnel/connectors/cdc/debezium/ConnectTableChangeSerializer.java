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

package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.debezium.relational.history.ConnectTableChangeSerializer.AUTO_INCREMENTED_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.CHARSET_NAME_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.COLUMNS_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.COMMENT_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.DEFAULT_CHARSET_NAME_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.GENERATED_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.ID_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.JDBC_TYPE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.LENGTH_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.NAME_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.NATIVE_TYPE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.OPTIONAL_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.POSITION_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.PRIMARY_KEY_COLUMN_NAMES_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.SCALE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TABLE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TYPE_EXPRESSION_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TYPE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TYPE_NAME_KEY;

/**
 * A serializer for {@link TableChanges} that deserialize the table list of {@link Struct} into a
 * {@link TableChanges}. This class is used to deserialize the checkpoint data into {@link
 * TableChanges}.
 */
@Slf4j
public class ConnectTableChangeSerializer
        implements TableChanges.TableChangesSerializer<List<Struct>>, Serializable {
    private static final String ENUM_VALUES_KEY = "enumValues";
    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    private static final Schema COLUMN_SCHEMA =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust("io.debezium.connector.schema.Column"))
                    .field(NAME_KEY, Schema.STRING_SCHEMA)
                    .field(JDBC_TYPE_KEY, Schema.INT32_SCHEMA)
                    .field(NATIVE_TYPE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                    .field(TYPE_NAME_KEY, Schema.STRING_SCHEMA)
                    .field(TYPE_EXPRESSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(LENGTH_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                    .field(SCALE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                    .field(POSITION_KEY, Schema.INT32_SCHEMA)
                    .field(OPTIONAL_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    .field(AUTO_INCREMENTED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    .field(GENERATED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    .field(COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(
                            ENUM_VALUES_KEY,
                            SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                    .build();

    public static final Schema TABLE_SCHEMA =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust("io.debezium.connector.schema.Table"))
                    .field(DEFAULT_CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(
                            PRIMARY_KEY_COLUMN_NAMES_KEY,
                            SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                    .field(COLUMNS_KEY, SchemaBuilder.array(COLUMN_SCHEMA).build())
                    .field(COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    public static final Schema CHANGE_SCHEMA =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust("io.debezium.connector.schema.Change"))
                    .field(TYPE_KEY, Schema.STRING_SCHEMA)
                    .field(ID_KEY, Schema.STRING_SCHEMA)
                    .field(TABLE_KEY, TABLE_SCHEMA)
                    .build();

    @Override
    public List<Struct> serialize(TableChanges tableChanges) {
        return StreamSupport.stream(tableChanges.spliterator(), false)
                .map(this::toStruct)
                .collect(Collectors.toList());
    }

    @Override
    public TableChanges deserialize(List<Struct> data, boolean useCatalogBeforeSchema) {
        TableChanges tableChanges = new TableChanges();
        for (Struct struct : data) {
            String tableId = struct.getString(ID_KEY);
            TableChanges.TableChangeType changeType =
                    TableChanges.TableChangeType.valueOf(struct.getString(TYPE_KEY));
            Table table = toTable(struct.getStruct(TABLE_KEY), TableId.parse(tableId));
            switch (changeType) {
                case CREATE:
                    tableChanges.create(table);
                    break;
                case DROP:
                    tableChanges.drop(table);
                    break;
                case ALTER:
                    tableChanges.alter(table);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown table change type: " + changeType);
            }
        }
        return tableChanges;
    }

    public Table toTable(Struct struct, TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .setDefaultCharsetName(struct.getString(DEFAULT_CHARSET_NAME_KEY))
                .setPrimaryKeyNames(struct.getArray(PRIMARY_KEY_COLUMN_NAMES_KEY))
                .setColumns(
                        struct.getArray(COLUMNS_KEY).stream()
                                .map(Struct.class::cast)
                                .map(this::toColumn)
                                .collect(Collectors.toList()))
                .create();
    }

    private Column toColumn(Struct struct) {
        ColumnEditor editor =
                Column.editor()
                        .name(struct.getString(NAME_KEY))
                        .jdbcType(struct.getInt32(JDBC_TYPE_KEY))
                        .type(
                                struct.getString(TYPE_NAME_KEY),
                                struct.getString(TYPE_EXPRESSION_KEY))
                        .charsetName(struct.getString(CHARSET_NAME_KEY))
                        .position(struct.getInt32(POSITION_KEY))
                        .optional(struct.getBoolean(OPTIONAL_KEY))
                        .autoIncremented(struct.getBoolean(AUTO_INCREMENTED_KEY))
                        .generated(struct.getBoolean(GENERATED_KEY));
        if (struct.get(NATIVE_TYPE_KEY) != null) {
            editor.nativeType(struct.getInt32(NATIVE_TYPE_KEY));
        }
        if (struct.get(LENGTH_KEY) != null) {
            editor.length(struct.getInt32(LENGTH_KEY));
        }
        if (struct.get(SCALE_KEY) != null) {
            editor.scale(struct.getInt32(SCALE_KEY));
        }
        if (struct.get(COMMENT_KEY) != null) {
            editor.comment(struct.getString(COMMENT_KEY));
        }
        if (struct.schema().field(ENUM_VALUES_KEY) != null) {
            editor.enumValues(struct.getArray(ENUM_VALUES_KEY));
        }
        return editor.create();
    }

    public Struct toStruct(TableChanges.TableChange tableChange) {
        final Struct struct = new Struct(CHANGE_SCHEMA);

        struct.put(TYPE_KEY, tableChange.getType().name());
        struct.put(ID_KEY, tableChange.getId().toDoubleQuotedString());
        struct.put(TABLE_KEY, toStruct(tableChange.getTable()));
        return struct;
    }

    private Struct toStruct(Table table) {
        final Struct struct = new Struct(TABLE_SCHEMA);

        struct.put(DEFAULT_CHARSET_NAME_KEY, table.defaultCharsetName());
        struct.put(PRIMARY_KEY_COLUMN_NAMES_KEY, table.primaryKeyColumnNames());

        final List<Struct> columns =
                table.columns().stream().map(this::toStruct).collect(Collectors.toList());

        struct.put(COLUMNS_KEY, columns);
        return struct;
    }

    private Struct toStruct(Column column) {
        final Struct struct = new Struct(COLUMN_SCHEMA);

        struct.put(NAME_KEY, column.name());
        struct.put(JDBC_TYPE_KEY, column.jdbcType());

        if (column.nativeType() != Column.UNSET_INT_VALUE) {
            struct.put(NATIVE_TYPE_KEY, column.nativeType());
        }

        struct.put(TYPE_NAME_KEY, column.typeName());
        struct.put(TYPE_EXPRESSION_KEY, column.typeExpression());
        struct.put(CHARSET_NAME_KEY, column.charsetName());

        if (column.length() != Column.UNSET_INT_VALUE) {
            struct.put(LENGTH_KEY, column.length());
        }

        column.scale().ifPresent(s -> struct.put(SCALE_KEY, s));

        struct.put(POSITION_KEY, column.position());
        struct.put(OPTIONAL_KEY, column.isOptional());
        struct.put(AUTO_INCREMENTED_KEY, column.isAutoIncremented());
        struct.put(GENERATED_KEY, column.isGenerated());
        struct.put(COMMENT_KEY, column.comment());
        struct.put(ENUM_VALUES_KEY, column.enumValues());

        return struct;
    }
}

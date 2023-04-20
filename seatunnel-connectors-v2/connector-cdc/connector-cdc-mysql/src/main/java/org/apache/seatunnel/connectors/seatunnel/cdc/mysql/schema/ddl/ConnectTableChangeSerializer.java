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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static io.debezium.relational.history.ConnectTableChangeSerializer.AUTO_INCREMENTED_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.CHARSET_NAME_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.COLUMNS_KEY;
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

public class ConnectTableChangeSerializer
        implements TableChanges.TableChangesSerializer<List<Struct>>, Serializable {

    private static final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

    private static final Schema COLUMN_SCHEMA =
            SchemaBuilder.struct()
                    .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Column"))
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
                    .build();

    @Override
    public List<Struct> serialize(TableChanges tableChanges) {
        throw new UnsupportedOperationException();
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
        return editor.create();
    }
}

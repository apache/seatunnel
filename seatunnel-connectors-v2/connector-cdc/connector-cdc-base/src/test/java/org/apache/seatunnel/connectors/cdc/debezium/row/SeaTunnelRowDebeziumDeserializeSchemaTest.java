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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SeaTunnelRowDebeziumDeserializeSchemaTest {

    @Test
    void deserializePopulatesRelationalSchemaMetadata() throws Exception {
        SeaTunnelRowDebeziumDeserializeSchema schema =
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(Collections.singletonList(catalogTable()))
                        .build();
        List<SeaTunnelRow> rows = new ArrayList<>();

        schema.deserialize(insertRecord("inventory", "public", "orders"), new ListCollector(rows));

        Assertions.assertEquals(1, rows.size());
        SeaTunnelRow row = rows.get(0);
        Assertions.assertEquals(RowKind.INSERT, row.getRowKind());
        Assertions.assertEquals("inventory.public.orders", row.getTableId());
        TablePath tablePath = TablePath.of(row.getTableId());
        Assertions.assertEquals("inventory", tablePath.getDatabaseName());
        Assertions.assertEquals("public", row.getOptions().get(CommonOptions.SCHEMA.getName()));
        Assertions.assertEquals("orders", tablePath.getTableName());
    }

    @Test
    void deserializeOmitsSchemaMetadataWhenSourceHasNoSchema() throws Exception {
        SeaTunnelRowDebeziumDeserializeSchema schema =
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(Collections.singletonList(catalogTable()))
                        .build();
        List<SeaTunnelRow> rows = new ArrayList<>();

        schema.deserialize(insertRecord("inventory", null, "orders"), new ListCollector(rows));

        Assertions.assertEquals(1, rows.size());
        Assertions.assertFalse(
                rows.get(0).getOptions().containsKey(CommonOptions.SCHEMA.getName()));
    }

    private CatalogTable catalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("catalog", "inventory", "public", "orders"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, true, null, ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "orders");
    }

    private SourceRecord insertRecord(String databaseName, String schemaName, String tableName) {
        Schema sourceSchema = sourceSchema(schemaName != null);
        Struct source =
                new Struct(sourceSchema)
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, databaseName)
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, tableName)
                        .put(Envelope.FieldName.TIMESTAMP, 100L);
        if (schemaName != null) {
            source.put(AbstractSourceInfo.SCHEMA_NAME_KEY, schemaName);
        }

        Schema rowSchema =
                SchemaBuilder.struct()
                        .name("test.Row")
                        .optional()
                        .field("id", Schema.INT32_SCHEMA)
                        .build();
        Struct after = new Struct(rowSchema).put("id", 1);
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.Envelope")
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                        .field(Envelope.FieldName.BEFORE, rowSchema)
                        .field(Envelope.FieldName.AFTER, rowSchema)
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c")
                        .put(Envelope.FieldName.TIMESTAMP, 200L)
                        .put(Envelope.FieldName.AFTER, after);

        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test-topic",
                null,
                null,
                null,
                valueSchema,
                value);
    }

    private Schema sourceSchema(boolean includeSchema) {
        SchemaBuilder builder =
                SchemaBuilder.struct()
                        .name("test.Source")
                        .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
        if (includeSchema) {
            builder.field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder.build();
    }

    private static class ListCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows;

        private ListCollector(List<SeaTunnelRow> rows) {
            this.rows = rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}

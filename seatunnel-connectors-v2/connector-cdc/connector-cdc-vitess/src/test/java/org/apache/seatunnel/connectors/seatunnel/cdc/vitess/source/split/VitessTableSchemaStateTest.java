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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Protects the schema-first table identity required by Vitess FIELD and restored ROW lookup. */
class VitessTableSchemaStateTest {

    @Test
    void shouldRestoreFieldSchemaWithTheDecoderTableIdentity() {
        Table table =
                Table.editor()
                        .tableId(new TableId(null, "commerce", "orders"))
                        .addColumn(
                                Column.editor()
                                        .name("ID")
                                        .jdbcType(Types.INTEGER)
                                        .type("INTEGER")
                                        .position(1)
                                        .optional(false)
                                        .create())
                        .setPrimaryKeyNames(Collections.singletonList("ID"))
                        .create();

        Table restored =
                VitessTableSchemaState.deserialize(VitessTableSchemaState.serialize(table));

        assertSchemaFirstIdentity(restored);
        Assertions.assertEquals(Collections.singletonList("ID"), restored.primaryKeyColumnNames());
        Assertions.assertEquals(Types.INTEGER, restored.columnWithName("ID").jdbcType());
        Assertions.assertFalse(restored.columnWithName("ID").isOptional());
    }

    @Test
    void shouldKeepCatalogBootstrapAndFieldHistoryInTheSameNamespace() {
        CatalogTable catalog =
                CatalogTable.of(
                        TableIdentifier.of("default", "commerce", null, "orders"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "ID", BasicType.INT_TYPE, 10L, false, null, null))
                                .primaryKey(PrimaryKey.of("PK", Collections.singletonList("ID")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        Map<String, byte[]> schemas =
                VitessTableSchemaState.serializeCatalogTables(Collections.singletonList(catalog));
        Assertions.assertEquals(Collections.singleton("\"commerce\".\"orders\""), schemas.keySet());

        Table restored = VitessTableSchemaState.deserialize(schemas.values().iterator().next());

        assertSchemaFirstIdentity(restored);
        Assertions.assertEquals(Collections.singletonList("ID"), restored.primaryKeyColumnNames());
        Assertions.assertEquals(Types.INTEGER, restored.columnWithName("ID").jdbcType());
    }

    @Test
    void shouldReadPreFixJsonLayoutWithTheDecoderTableIdentity() {
        // Fixed layout from the pre-PR CHANGE_SCHEMA and writer; no current serializer builds it.
        byte[] checkpoint =
                ("{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"type\"},"
                                + "{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\","
                                + "\"optional\":true,\"field\":\"defaultCharsetName\"},{\"type\":\"array\",\"items\":{\"type\":\"string\","
                                + "\"optional\":false},\"optional\":true,\"field\":\"primaryKeyColumnNames\"},{\"type\":\"array\","
                                + "\"items\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},"
                                + "{\"type\":\"int32\",\"optional\":false,\"field\":\"jdbcType\"},{\"type\":\"int32\",\"optional\":true,"
                                + "\"field\":\"nativeType\"},{\"type\":\"string\",\"optional\":false,\"field\":\"typeName\"},{\"type\":\"string\","
                                + "\"optional\":true,\"field\":\"typeExpression\"},{\"type\":\"string\",\"optional\":true,\"field\":\"charsetName\"},"
                                + "{\"type\":\"int32\",\"optional\":true,\"field\":\"length\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"scale\"},"
                                + "{\"type\":\"int32\",\"optional\":false,\"field\":\"position\"},{\"type\":\"boolean\",\"optional\":true,"
                                + "\"field\":\"optional\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"autoIncremented\"},{\"type\":\"boolean\","
                                + "\"optional\":true,\"field\":\"generated\"},{\"type\":\"string\",\"optional\":true,\"field\":\"comment\"},"
                                + "{\"type\":\"array\",\"items\":{\"type\":\"string\",\"optional\":false},\"optional\":true,\"field\":\"enumValues\"}],"
                                + "\"optional\":false,\"name\":\"io.debezium.connector.schema.Column\"},\"optional\":false,\"field\":\"columns\"},"
                                + "{\"type\":\"string\",\"optional\":true,\"field\":\"comment\"}],\"optional\":false,"
                                + "\"name\":\"io.debezium.connector.schema.Table\",\"field\":\"table\"}],\"optional\":false,"
                                + "\"name\":\"io.debezium.connector.schema.Change\"},\"payload\":{\"type\":\"CREATE\","
                                + "\"id\":\"\\\"commerce\\\".\\\"orders\\\"\",\"table\":{\"defaultCharsetName\":null,\"primaryKeyColumnNames\":[\"ID\"],"
                                + "\"columns\":[{\"name\":\"ID\",\"jdbcType\":4,\"nativeType\":null,\"typeName\":\"INTEGER\","
                                + "\"typeExpression\":\"INTEGER\",\"charsetName\":null,\"length\":null,\"scale\":null,\"position\":1,"
                                + "\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"enumValues\":[]}],"
                                + "\"comment\":null}}}")
                        .getBytes(StandardCharsets.UTF_8);

        Table restored = VitessTableSchemaState.deserialize(checkpoint);

        assertSchemaFirstIdentity(restored);
        Assertions.assertEquals(Collections.singletonList("ID"), restored.primaryKeyColumnNames());
        Assertions.assertEquals(Types.INTEGER, restored.columnWithName("ID").jdbcType());
        Assertions.assertFalse(restored.columnWithName("ID").isOptional());

        // The namespace correction must not change the existing checkpoint write layout.
        try (JsonConverter converter = new JsonConverter()) {
            converter.configure(Collections.singletonMap("schemas.enable", true), false);
            SchemaAndValue historical = converter.toConnectData("history", checkpoint);
            SchemaAndValue rewritten =
                    converter.toConnectData("history", VitessTableSchemaState.serialize(restored));
            Assertions.assertEquals(historical.schema(), rewritten.schema());
            Assertions.assertEquals(historical.value(), rewritten.value());
        }
    }

    private static void assertSchemaFirstIdentity(Table table) {
        // TableId.equals compares the combined name, hiding a swapped catalog/schema component.
        Assertions.assertNull(table.id().catalog());
        Assertions.assertEquals("commerce", table.id().schema());
        Assertions.assertEquals("orders", table.id().table());
        TableId decoderId = VitessDatabaseSchema.parse("commerce.orders");
        Assertions.assertEquals(decoderId.catalog(), table.id().catalog());
        Assertions.assertEquals(decoderId.schema(), table.id().schema());
        Assertions.assertEquals(decoderId.table(), table.id().table());
    }
}

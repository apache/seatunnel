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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

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

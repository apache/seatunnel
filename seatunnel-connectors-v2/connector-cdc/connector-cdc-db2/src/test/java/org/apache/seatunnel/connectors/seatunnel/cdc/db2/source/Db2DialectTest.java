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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;

/** Protects catalog-free DB2 key lookup and absent catalog entries during split planning. */
class Db2DialectTest {

    @Test
    void shouldReturnEmptyKeysForMissingCatalogTable() {
        Db2Dialect dialect = dialect(null);
        TableId table = new TableId("", "DB2INST1", "UNKNOWN");

        Assertions.assertEquals(Collections.emptyList(), dialect.getConstraintKeys(null, table));
        Assertions.assertFalse(dialect.getPrimaryKey(null, table).isPresent());
    }

    @Test
    void shouldPreserveConfiguredKeysWithoutDatabaseSegment() {
        PrimaryKey primaryKey = PrimaryKey.of("PK", Collections.singletonList("ID"));
        ConstraintKey uniqueKey =
                ConstraintKey.of(
                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                        "UK",
                        Collections.singletonList(
                                ConstraintKey.ConstraintKeyColumn.of(
                                        "ID", ConstraintKey.ColumnSortType.ASC)));
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("default", "E2E", "DB2INST1", "CUSTOMERS"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "ID", BasicType.INT_TYPE, 10L, false, null, null))
                                .primaryKey(primaryKey)
                                .constraintKey(Collections.singletonList(uniqueKey))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        Db2Dialect dialect = dialect(table);
        TableId runtimeId = new TableId("", "DB2INST1", "CUSTOMERS");

        Assertions.assertEquals(
                Collections.singletonList("ID"),
                dialect.getPrimaryKey(null, runtimeId).get().getColumnNames());
        Assertions.assertEquals(
                Collections.singletonList(uniqueKey), dialect.getConstraintKeys(null, runtimeId));
    }

    private static Db2Dialect dialect(CatalogTable table) {
        Db2SourceConfigFactory factory = Mockito.mock(Db2SourceConfigFactory.class);
        Mockito.when(factory.create(0)).thenReturn(Mockito.mock(Db2SourceConfig.class));
        return new Db2Dialect(
                factory,
                table == null ? Collections.emptyList() : Collections.singletonList(table));
    }
}

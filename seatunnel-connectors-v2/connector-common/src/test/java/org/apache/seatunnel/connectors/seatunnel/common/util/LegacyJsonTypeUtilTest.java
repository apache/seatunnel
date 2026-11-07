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

package org.apache.seatunnel.connectors.seatunnel.common.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

/**
 * Guards the {@code handle_json_as_string} opt-out: an inferred source schema that contains native
 * JSON columns must come back as the legacy {@code STRING} schema while every other column, the
 * primary key and the column metadata stay untouched.
 */
class LegacyJsonTypeUtilTest {

    @Test
    void jsonColumnsAreRemappedToStringAndOtherColumnsArePreserved() {
        Column id = PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, 0, false, null, "id");
        Column payload =
                PhysicalColumn.of(
                        "payload", BasicType.JSON_TYPE, null, null, true, null, "payload");
        CatalogTable source =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", null, "t"),
                        TableSchema.builder()
                                .column(id)
                                .column(payload)
                                .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "comment");

        CatalogTable legacy = LegacyJsonTypeUtil.mapJsonColumnsToString(source);

        TableSchema schema = legacy.getTableSchema();
        Assertions.assertEquals(
                SqlType.STRING, schema.getColumn("payload").getDataType().getSqlType());
        Assertions.assertEquals("payload", schema.getColumn("payload").getComment());
        Assertions.assertTrue(schema.getColumn("payload").isNullable());
        Assertions.assertSame(id, schema.getColumn("id"));
        Assertions.assertEquals(
                Collections.singletonList("id"), schema.getPrimaryKey().getColumnNames());
        Assertions.assertEquals(source.getTableId(), legacy.getTableId());
        Assertions.assertEquals("comment", legacy.getComment());
        // The JSON logical type must still be produced when the opt-out is not applied.
        Assertions.assertEquals(
                SqlType.JSON,
                source.getTableSchema().getColumn("payload").getDataType().getSqlType());
    }

    @Test
    void tableWithoutJsonColumnsIsReturnedUnchanged() {
        CatalogTable source =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", null, "t"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                64L,
                                                0,
                                                true,
                                                null,
                                                ""))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");

        Assertions.assertSame(source, LegacyJsonTypeUtil.mapJsonColumnsToString(source));
    }
}

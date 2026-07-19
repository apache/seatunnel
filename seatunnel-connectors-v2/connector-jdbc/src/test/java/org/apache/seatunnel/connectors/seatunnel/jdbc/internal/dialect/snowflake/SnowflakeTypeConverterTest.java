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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the bidirectional mapping between Snowflake VARIANT and SeaTunnel JSON. */
public class SnowflakeTypeConverterTest {

    /** Verifies that Snowflake VARIANT columns use the JSON logical type. */
    @Test
    public void testVariantMapsToJsonType() {
        BasicTypeDefine<?> typeDefine =
                BasicTypeDefine.builder()
                        .name("payload")
                        .columnType("VARIANT")
                        .dataType("VARIANT")
                        .build();

        Column column = SnowflakeTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(BasicType.JSON_TYPE, column.getDataType());
    }

    /** Verifies that the JSON logical type is recreated as Snowflake VARIANT. */
    @Test
    public void testJsonTypeMapsToVariant() {
        Column column =
                PhysicalColumn.of("payload", BasicType.JSON_TYPE, (Long) null, true, null, null);

        BasicTypeDefine<?> typeDefine = SnowflakeTypeConverter.INSTANCE.reconvert(column);

        Assertions.assertEquals("VARIANT", typeDefine.getDataType());
        Assertions.assertEquals("VARIANT", typeDefine.getColumnType());
    }

    /** Verifies Snowflake inserts parse JSON parameters into structured VARIANT values. */
    @Test
    public void testJsonInsertUsesParseJson() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "payload",
                                        BasicType.JSON_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();

        String insertSql =
                new SnowflakeDialect().getInsertIntoStatement("database", "events", tableSchema);

        Assertions.assertEquals(
                "INSERT INTO database.events (id, payload) VALUES (:id, PARSE_JSON(:payload))",
                insertSql);
    }

    /** Verifies Snowflake updates parse JSON parameters into structured VARIANT values. */
    @Test
    public void testJsonUpdateUsesParseJson() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "payload",
                                        BasicType.JSON_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();

        String updateSql =
                new SnowflakeDialect()
                        .getUpdateStatement(
                                "database", "events", tableSchema, new String[] {"id"}, false);

        Assertions.assertEquals(
                "UPDATE database.events SET payload = PARSE_JSON(:payload) WHERE id = :id",
                updateSql);
    }
}

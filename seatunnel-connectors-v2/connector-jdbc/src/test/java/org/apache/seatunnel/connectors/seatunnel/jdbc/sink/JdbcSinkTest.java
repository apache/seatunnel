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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class JdbcSinkTest {

    @Test
    void getPrimaryKeyIndexReturnsEmptyForPrimaryKeyWithoutColumns() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .primaryKey(PrimaryKey.of("empty_pk", Collections.emptyList()))
                        .build();

        Assertions.assertFalse(JdbcSink.getPrimaryKeyIndex(tableSchema, null).isPresent());
    }

    @Test
    void getPrimaryKeyIndexReturnsEmptyForPrimaryKeyWithNullColumns() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .primaryKey(PrimaryKey.of("null_pk", null))
                        .build();

        Assertions.assertFalse(JdbcSink.getPrimaryKeyIndex(tableSchema, null).isPresent());
    }

    @Test
    void getPrimaryKeyIndexReturnsFirstPrimaryKeyColumnIndex() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .primaryKey(PrimaryKey.of("id_pk", Collections.singletonList("id")))
                        .build();

        Assertions.assertEquals(1, JdbcSink.getPrimaryKeyIndex(tableSchema, null).get());
    }

    @Test
    void getPrimaryKeyIndexFallsBackToTheSinkKeyWhenTheSchemaHasNone() {
        // A table with only a UNIQUE key: the factory sets primary_keys from it, so the sink
        // upserts by "code", and the rows of one code must be routed by it.
        TableSchema tableSchema = nameAndCodeSchema(null);

        Assertions.assertEquals(
                1, JdbcSink.getPrimaryKeyIndex(tableSchema, Arrays.asList("code", "name")).get());
    }

    @Test
    void getPrimaryKeyIndexPrefersTheSchemaPrimaryKey() {
        TableSchema tableSchema =
                nameAndCodeSchema(PrimaryKey.of("name_pk", Collections.singletonList("name")));

        Assertions.assertEquals(
                0,
                JdbcSink.getPrimaryKeyIndex(tableSchema, Collections.singletonList("code")).get());
    }

    @Test
    void getPrimaryKeyIndexIsEmptyWithoutAnyKey() {
        TableSchema tableSchema = nameAndCodeSchema(null);

        Assertions.assertFalse(JdbcSink.getPrimaryKeyIndex(tableSchema, null).isPresent());
        Assertions.assertFalse(
                JdbcSink.getPrimaryKeyIndex(tableSchema, Collections.emptyList()).isPresent());
    }

    @Test
    void getPrimaryKeyIndexIgnoresASinkKeyThatIsNotInTheRow() {
        TableSchema tableSchema = nameAndCodeSchema(null);

        Assertions.assertFalse(
                JdbcSink.getPrimaryKeyIndex(tableSchema, Collections.singletonList("missing"))
                        .isPresent());
    }

    private static TableSchema nameAndCodeSchema(PrimaryKey primaryKey) {
        TableSchema.Builder builder =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("code")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build());
        if (primaryKey != null) {
            builder.primaryKey(primaryKey);
        }
        return builder.build();
    }
}

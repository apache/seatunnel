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

package org.apache.seatunnel.transform.fieldmapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.fieldmapper.FieldMapperTransformConfig.FIELD_MAPPER;

class FieldMapperTransformTest {
    static CatalogTable catalogTable;

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .primaryKey(PrimaryKey.of("pk", Arrays.asList("key1", "key2")))
                                .constraintKey(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                "uk",
                                                Arrays.asList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "key1",
                                                                ConstraintKey.ColumnSortType.ASC),
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "key3",
                                                                ConstraintKey.ColumnSortType.ASC))))
                                .build(),
                        new HashMap<>(),
                        Collections.singletonList("key1"),
                        "comment");
    }

    @Test
    void transformTableSchema() {
        Map<String, String> mapper = new HashMap<>();
        mapper.put("key1", "k1");
        mapper.put("key2", "key2");
        mapper.put("key3", "key3");
        mapper.put("key4", "k4");

        Map<String, Object> config = Collections.singletonMap(FIELD_MAPPER.key(), mapper);
        FieldMapperTransform transform =
                new FieldMapperTransform(
                        FieldMapperTransformConfig.of(ReadonlyConfig.fromMap(config)),
                        catalogTable);

        TableSchema newSchema = transform.getProducedCatalogTable().getTableSchema();

        Assertions.assertEquals(4, newSchema.getColumns().size());
        Assertions.assertArrayEquals(
                new String[] {"k1", "key2", "key3", "k4"}, newSchema.getFieldNames());
        Assertions.assertIterableEquals(
                Arrays.asList("k1", "key2"), newSchema.getPrimaryKey().getColumnNames());
        List<ConstraintKey> newConstraintKeys = newSchema.getConstraintKeys();
        Assertions.assertEquals(1, newConstraintKeys.size());
        Assertions.assertIterableEquals(
                Arrays.asList("k1", "key3"),
                newConstraintKeys.get(0).getColumnNames().stream()
                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                        .collect(Collectors.toList()));
    }
}

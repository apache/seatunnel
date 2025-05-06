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

package org.apache.seatunnel.transform.structevolution;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StructEvolutionTransformTest {

    static CatalogTable catalogTable;
    static Object[] values;
    static SeaTunnelRow inputRow;

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
                                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
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
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values =
                new Object[] {
                    "1",
                    "value2",
                    LocalDateTime.of(2000, 10, 29, 10, 29, 11, 111111000),
                    LocalDateTime.of(2000, 10, 29, 10, 29, 11, 111111000),
                    "value5"
                };
        inputRow = new SeaTunnelRow(values);
    }

    @Test
    public void testStructEvolutionTransformConfig() throws JsonProcessingException {

        List<StructEvolutionConfig.SpecificModify> specificModifies =
                Lists.newArrayList(
                        new StructEvolutionConfig.SpecificModify(
                                "default.default",
                                "schema.table",
                                Lists.newArrayList(
                                        StructEvolutionConfig.Column.builder()
                                                .position(2)
                                                .inputName("key1")
                                                .outputName("id")
                                                .dataType(SqlType.INT)
                                                .length(10L)
                                                .scale(0)
                                                .nullable(false)
                                                .outputType("INT")
                                                .defaultValue(null)
                                                .comment("主键ID")
                                                .action(StructEvolutionConfig.Action.MODIFY)
                                                .build(),
                                        StructEvolutionConfig.Column.builder()
                                                .position(1)
                                                .inputName("key2")
                                                .outputName("name")
                                                .dataType(SqlType.STRING)
                                                .length(255L)
                                                .scale(0)
                                                .nullable(true)
                                                .outputType("VARCHAR(255)")
                                                .defaultValue("")
                                                .comment("用户全名")
                                                .action(StructEvolutionConfig.Action.MODIFY)
                                                .build(),
                                        StructEvolutionConfig.Column.builder()
                                                .position(3)
                                                .inputName("key3")
                                                .outputName("time1")
                                                .dataType(SqlType.STRING)
                                                .length(null)
                                                .scale(null)
                                                .nullable(true)
                                                .outputType("")
                                                .defaultValue(null)
                                                .comment("时间1")
                                                .action(StructEvolutionConfig.Action.MODIFY)
                                                .build(),
                                        StructEvolutionConfig.Column.builder()
                                                .position(4)
                                                .inputName("key4")
                                                .outputName("time2")
                                                .dataType(SqlType.TIMESTAMP)
                                                .length(null)
                                                .scale(null)
                                                .nullable(true)
                                                .outputType("")
                                                .defaultValue(null)
                                                .comment("时间2")
                                                .action(StructEvolutionConfig.Action.MODIFY)
                                                .build()),
                                new StructEvolutionConfig.Primarykey(
                                        "pk_id",
                                        "pk_id",
                                        Lists.newArrayList(
                                                new StructEvolutionConfig.ReferenceColumn(
                                                        "id", ConstraintKey.ColumnSortType.ASC)),
                                        StructEvolutionConfig.Action.ADD),
                                Lists.newArrayList(
                                        new StructEvolutionConfig.Index(
                                                "idx_full_name",
                                                false,
                                                Lists.newArrayList(
                                                        new StructEvolutionConfig.ReferenceColumn(
                                                                "full_name",
                                                                ConstraintKey.ColumnSortType.ASC)),
                                                StructEvolutionConfig.Action.ADD)),
                                null,
                                new StructEvolutionConfig.Comment(
                                        "用户信息表，包含基础用户数据", StructEvolutionConfig.Action.ADD)));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(StructEvolutionConfig.SPECIFIC.key(), specificModifies);
                            }
                        });

        StructEvolutionTransform StructEvolutionTransform =
                new StructEvolutionTransform(config, catalogTable);
        Assertions.assertEquals(
                "default.schema.table",
                StructEvolutionTransform.transformTableIdentifier().toTablePath().getFullName());
        Assertions.assertIterableEquals(
                Arrays.asList("name", "id", "time1", "time2", "key5"),
                Arrays.asList(
                        Arrays.stream(StructEvolutionTransform.getOutputColumns())
                                .map(Column::getName)
                                .toArray(String[]::new)));
        Assertions.assertIterableEquals(
                Arrays.asList(
                        "value2",
                        "1",
                        LocalDateTime.of(2000, 10, 29, 10, 29, 11, 111111000),
                        LocalDateTime.of(2000, 10, 29, 10, 29, 11, 111111000),
                        "value5"),
                Arrays.asList(
                        StructEvolutionTransform.getOutputFieldValues(
                                new SeaTunnelRowAccessor(inputRow))));
    }
}

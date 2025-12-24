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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class MaxComputeCreateTableTest {

    @Test
    public void test() {

        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "age", BasicType.INT_TYPE, (Long) null, true, null, "test comment"));
        columns.add(PhysicalColumn.of("score", BasicType.INT_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("gender", BasicType.BYTE_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of("create_time", BasicType.LONG_TYPE, (Long) null, true, null, ""));

        String result =
                MaxComputeCatalogUtil.getCreateTableStatement(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (                                                                                                                                                   \n"
                                + "${rowtype_primary_key}  ,       \n"
                                + "${rowtype_unique_key} , \n"
                                + "`create_time` DATETIME NOT NULL ,  \n"
                                + "${rowtype_fields}  \n"
                                + ") ENGINE=OLAP  \n"
                                + "PRIMARY KEY(${rowtype_primary_key},`create_time`)  \n"
                                + "PARTITION BY RANGE (`create_time`)(  \n"
                                + "   PARTITION p20230329 VALUES LESS THAN (\"2023-03-29\")                                                                                                                                                           \n"
                                + ")                                      \n"
                                + "DISTRIBUTED BY HASH (${rowtype_primary_key})  \n"
                                + "PROPERTIES (\n"
                                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                                + "\"in_memory\" = \"false\",\n"
                                + "\"storage_format\" = \"V2\",\n"
                                + "\"disable_auto_compaction\" = \"false\"\n"
                                + ") COMMENT '${comment}';",
                        TablePath.of("test1.test2"),
                        CatalogTable.of(
                                TableIdentifier.of("test", "test1", "test2"),
                                TableSchema.builder()
                                        .primaryKey(PrimaryKey.of("", Arrays.asList("id", "age")))
                                        .constraintKey(
                                                Arrays.asList(
                                                        ConstraintKey.of(
                                                                ConstraintKey.ConstraintType
                                                                        .UNIQUE_KEY,
                                                                "unique_key",
                                                                Collections.singletonList(
                                                                        ConstraintKey
                                                                                .ConstraintKeyColumn
                                                                                .of(
                                                                                        "name",
                                                                                        ConstraintKey
                                                                                                .ColumnSortType
                                                                                                .DESC))),
                                                        ConstraintKey.of(
                                                                ConstraintKey.ConstraintType
                                                                        .UNIQUE_KEY,
                                                                "unique_key2",
                                                                Collections.singletonList(
                                                                        ConstraintKey
                                                                                .ConstraintKeyColumn
                                                                                .of(
                                                                                        "score",
                                                                                        ConstraintKey
                                                                                                .ColumnSortType
                                                                                                .ASC)))))
                                        .columns(columns)
                                        .build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "comment"));
        Assertions.assertEquals(
                result,
                "CREATE TABLE IF NOT EXISTS `test1`.`test2` (                                                                                                                                                   \n"
                        + "`id` BIGINT NULL ,`age` INT NULL COMMENT 'test comment'  ,       \n"
                        + "`name` STRING NULL ,`score` INT NULL  , \n"
                        + "`create_time` DATETIME NOT NULL ,  \n"
                        + "`gender` TINYINT NULL   \n"
                        + ") ENGINE=OLAP  \n"
                        + "PRIMARY KEY(`id`,`age`,`create_time`)  \n"
                        + "PARTITION BY RANGE (`create_time`)(  \n"
                        + "   PARTITION p20230329 VALUES LESS THAN (\"2023-03-29\")                                                                                                                                                           \n"
                        + ")                                      \n"
                        + "DISTRIBUTED BY HASH (`id`,`age`)  \n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"in_memory\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"disable_auto_compaction\" = \"false\"\n"
                        + ") COMMENT 'comment';");
    }
}

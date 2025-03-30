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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.doris.config.DorisSinkOptions;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV1;
import org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DorisCreateTableTest {

    @Test
    public void test() {

        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "age", BasicType.INT_TYPE, (Long) null, true, null, "test comment"));
        columns.add(
                PhysicalColumn.of("score", BasicType.INT_TYPE, (Long) null, true, null, "'N'-N"));
        columns.add(PhysicalColumn.of("gender", BasicType.BYTE_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of("create_time", BasicType.LONG_TYPE, (Long) null, true, null, ""));

        String result =
                DorisCatalogUtil.getCreateTableStatement(
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
                                + ")",
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
                                "doris test comment"),
                        DorisTypeConverterV1.INSTANCE);
        Assertions.assertEquals(
                result,
                "CREATE TABLE IF NOT EXISTS `test1`.`test2` (                                                                                                                                                   \n"
                        + "`id` BIGINT NULL ,`age` INT NULL COMMENT 'test comment'  ,       \n"
                        + "`name` STRING NULL ,`score` INT NULL COMMENT '''N''-N' , \n"
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
                        + ")");

        String createTemplate = DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE.defaultValue();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test", "test1", "test2"),
                        TableSchema.builder()
                                .primaryKey(
                                        PrimaryKey.of(StringUtils.EMPTY, Collections.emptyList()))
                                .constraintKey(Collections.emptyList())
                                .columns(columns)
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "doris test comment");
        TablePath tablePath = TablePath.of("test1.test2");
        SeaTunnelRuntimeException actualSeaTunnelRuntimeException =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                DorisCatalogUtil.getCreateTableStatement(
                                        createTemplate,
                                        tablePath,
                                        catalogTable,
                                        DorisTypeConverterV1.INSTANCE));
        String primaryKeyHolder = SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder();
        SeaTunnelRuntimeException exceptSeaTunnelRuntimeException =
                CommonError.sqlTemplateHandledError(
                        tablePath.getFullName(),
                        SaveModePlaceHolder.getDisplay(primaryKeyHolder),
                        createTemplate,
                        primaryKeyHolder,
                        DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
        Assertions.assertEquals(
                exceptSeaTunnelRuntimeException.getMessage(),
                actualSeaTunnelRuntimeException.getMessage());
    }

    @Test
    public void testInSeq() {

        List<Column> columns = new ArrayList<>();

        columns.add(
                PhysicalColumn.of("L_ORDERKEY", BasicType.INT_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of("L_PARTKEY", BasicType.INT_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of("L_SUPPKEY", BasicType.INT_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_LINENUMBER", BasicType.INT_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_QUANTITY", new DecimalType(15, 2), (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_EXTENDEDPRICE", new DecimalType(15, 2), (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_DISCOUNT", new DecimalType(15, 2), (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of("L_TAX", new DecimalType(15, 2), (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_RETURNFLAG", BasicType.STRING_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_LINESTATUS", BasicType.STRING_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_SHIPDATE", LocalTimeType.LOCAL_DATE_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_COMMITDATE",
                        LocalTimeType.LOCAL_DATE_TYPE,
                        (Long) null,
                        false,
                        null,
                        ""));
        columns.add(
                PhysicalColumn.of(
                        "L_RECEIPTDATE",
                        LocalTimeType.LOCAL_DATE_TYPE,
                        (Long) null,
                        false,
                        null,
                        ""));
        columns.add(
                PhysicalColumn.of(
                        "L_SHIPINSTRUCT", BasicType.STRING_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_SHIPMODE", BasicType.STRING_TYPE, (Long) null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_COMMENT", BasicType.STRING_TYPE, (Long) null, false, null, ""));

        String result =
                DorisCatalogUtil.getCreateTableStatement(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                                + "`L_COMMITDATE`,\n"
                                + "${rowtype_primary_key},\n"
                                + "L_SUPPKEY BIGINT NOT NULL,\n"
                                + "${rowtype_fields}\n"
                                + ") ENGINE=OLAP\n"
                                + " PRIMARY KEY (L_COMMITDATE, ${rowtype_primary_key}, L_SUPPKEY)\n"
                                + "DISTRIBUTED BY HASH (${rowtype_primary_key})"
                                + "PROPERTIES (\n"
                                + "    \"replication_num\" = \"1\" \n"
                                + ")",
                        TablePath.of("tpch", "lineitem"),
                        CatalogTable.of(
                                TableIdentifier.of("test", "tpch", "lineitem"),
                                TableSchema.builder()
                                        .primaryKey(
                                                PrimaryKey.of(
                                                        "",
                                                        Arrays.asList(
                                                                "L_ORDERKEY", "L_LINENUMBER")))
                                        .columns(columns)
                                        .build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "doris test comment"),
                        DorisTypeConverterV1.INSTANCE);
        String expected =
                "CREATE TABLE IF NOT EXISTS `tpch`.`lineitem` (\n"
                        + "`L_COMMITDATE` DATEV2 NOT NULL ,\n"
                        + "`L_ORDERKEY` INT NOT NULL ,`L_LINENUMBER` INT NOT NULL ,\n"
                        + "L_SUPPKEY BIGINT NOT NULL,\n"
                        + "`L_PARTKEY` INT NOT NULL ,\n"
                        + "`L_QUANTITY` DECIMALV3(15,2) NOT NULL ,\n"
                        + "`L_EXTENDEDPRICE` DECIMALV3(15,2) NOT NULL ,\n"
                        + "`L_DISCOUNT` DECIMALV3(15,2) NOT NULL ,\n"
                        + "`L_TAX` DECIMALV3(15,2) NOT NULL ,\n"
                        + "`L_RETURNFLAG` STRING NOT NULL ,\n"
                        + "`L_LINESTATUS` STRING NOT NULL ,\n"
                        + "`L_SHIPDATE` DATEV2 NOT NULL ,\n"
                        + "`L_RECEIPTDATE` DATEV2 NOT NULL ,\n"
                        + "`L_SHIPINSTRUCT` STRING NOT NULL ,\n"
                        + "`L_SHIPMODE` STRING NOT NULL ,\n"
                        + "`L_COMMENT` STRING NOT NULL \n"
                        + ") ENGINE=OLAP\n"
                        + " PRIMARY KEY (L_COMMITDATE, `L_ORDERKEY`,`L_LINENUMBER`, L_SUPPKEY)\n"
                        + "DISTRIBUTED BY HASH (`L_ORDERKEY`,`L_LINENUMBER`)PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\" \n"
                        + ")";
        Assertions.assertEquals(result, expected);
    }

    @Test
    public void testWithVarchar() {

        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("comment", BasicType.STRING_TYPE, 500, true, null, ""));
        columns.add(PhysicalColumn.of("description", BasicType.STRING_TYPE, 70000, true, null, ""));

        String result =
                DorisCatalogUtil.getCreateTableStatement(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (                                                                                                                                                   \n"
                                + "${rowtype_primary_key}  ,       \n"
                                + "`create_time` DATETIME NOT NULL ,  \n"
                                + "${rowtype_fields}  \n"
                                + ") ENGINE=OLAP  \n"
                                + "PRIMARY KEY(${rowtype_primary_key},`create_time`)  \n"
                                + "PARTITION BY RANGE (`create_time`)(  \n"
                                + "   PARTITION p20230329 VALUES LESS THAN (\"2023-03-29\")                                                                                                                                                           \n"
                                + ")                                      \n"
                                + "DISTRIBUTED BY HASH (${rowtype_primary_key})  \n"
                                + "PROPERTIES (                           \n"
                                + "    \"dynamic_partition.enable\" = \"true\",                                                                                                                                                                       \n"
                                + "    \"dynamic_partition.time_unit\" = \"DAY\",                                                                                                                                                                     \n"
                                + "    \"dynamic_partition.end\" = \"3\", \n"
                                + "    \"dynamic_partition.prefix\" = \"p\"                                                                                                                                                                           \n"
                                + ");",
                        TablePath.of("test1", "test2"),
                        CatalogTable.of(
                                TableIdentifier.of("test", "test1", "test2"),
                                TableSchema.builder()
                                        .primaryKey(PrimaryKey.of("", Arrays.asList("id", "age")))
                                        .columns(columns)
                                        .build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "doris test comment"),
                        DorisTypeConverterV1.INSTANCE);

        Assertions.assertEquals(
                result,
                "CREATE TABLE IF NOT EXISTS `test1`.`test2` (                                                                                                                                                   \n"
                        + "`id` BIGINT NULL ,`age` INT NULL   ,       \n"
                        + "`create_time` DATETIME NOT NULL ,  \n"
                        + "`name` STRING NULL ,\n"
                        + "`comment` VARCHAR(500) NULL ,\n"
                        + "`description` STRING NULL   \n"
                        + ") ENGINE=OLAP  \n"
                        + "PRIMARY KEY(`id`,`age`,`create_time`)  \n"
                        + "PARTITION BY RANGE (`create_time`)(  \n"
                        + "   PARTITION p20230329 VALUES LESS THAN (\"2023-03-29\")                                                                                                                                                           \n"
                        + ")                                      \n"
                        + "DISTRIBUTED BY HASH (`id`,`age`)  \n"
                        + "PROPERTIES (                           \n"
                        + "    \"dynamic_partition.enable\" = \"true\",                                                                                                                                                                       \n"
                        + "    \"dynamic_partition.time_unit\" = \"DAY\",                                                                                                                                                                     \n"
                        + "    \"dynamic_partition.end\" = \"3\", \n"
                        + "    \"dynamic_partition.prefix\" = \"p\"                                                                                                                                                                           \n"
                        + ");");
    }

    @Test
    public void testWithThreePrimaryKeys() {
        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, true, null, ""));
        columns.add(PhysicalColumn.of("comment", BasicType.STRING_TYPE, 500, true, null, ""));
        columns.add(PhysicalColumn.of("description", BasicType.STRING_TYPE, 70000, true, null, ""));

        String result =
                DorisCatalogUtil.getCreateTableStatement(
                        "create table '${database}'.'${table}'(\n"
                                + "     ${rowtype_fields}\n"
                                + " )\n"
                                + " partitioned by ${rowtype_primary_key};",
                        TablePath.of("test1", "test2"),
                        CatalogTable.of(
                                TableIdentifier.of("test", "test1", "test2"),
                                TableSchema.builder()
                                        .primaryKey(
                                                PrimaryKey.of(
                                                        "test", Arrays.asList("id", "age", "name")))
                                        .columns(columns)
                                        .build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "doris test comment"),
                        DorisTypeConverterV1.INSTANCE);

        Assertions.assertEquals(
                "create table 'test1'.'test2'(\n"
                        + "     `id` BIGINT NULL ,\n"
                        + "`name` STRING NULL ,\n"
                        + "`age` INT NULL ,\n"
                        + "`comment` VARCHAR(500) NULL ,\n"
                        + "`description` STRING NULL \n"
                        + " )\n"
                        + " partitioned by `id`,`age`,`name`;",
                result);
    }

    @Test
    public void testTableComment() {
        List<Column> columns = new ArrayList<>();

        columns.add(
                PhysicalColumn.of(
                        "id",
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        "This is the ID column"));
        columns.add(
                PhysicalColumn.of(
                        "name",
                        BasicType.STRING_TYPE,
                        (Long) null,
                        true,
                        null,
                        "This is the name column"));
        columns.add(
                PhysicalColumn.of(
                        "age",
                        BasicType.INT_TYPE,
                        (Long) null,
                        true,
                        null,
                        "This is the age column"));
        columns.add(PhysicalColumn.of("score", BasicType.INT_TYPE, (Long) null, true, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "gender",
                        BasicType.BYTE_TYPE,
                        (Long) null,
                        true,
                        null,
                        "This is the gender column"));
        columns.add(
                PhysicalColumn.of(
                        "create_time",
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        "This is the create_time column"));

        String result =
                DorisCatalogUtil.getCreateTableStatement(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                                + "${rowtype_primary_key},\n"
                                + "${rowtype_fields}\n"
                                + ") ENGINE=OLAP\n"
                                + " UNIQUE KEY (${rowtype_primary_key})\n"
                                + "COMMENT '${comment}'\n"
                                + "DISTRIBUTED BY HASH (${rowtype_primary_key})\n"
                                + " PROPERTIES (\n"
                                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                                + "\"in_memory\" = \"false\",\n"
                                + "\"storage_format\" = \"V2\",\n"
                                + "\"disable_auto_compaction\" = \"false\"\n"
                                + ")",
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
                                "doris test comment"),
                        DorisTypeConverterV1.INSTANCE);

        Assertions.assertEquals(
                result,
                "CREATE TABLE IF NOT EXISTS `test1`.`test2` (\n"
                        + "`id` BIGINT NULL COMMENT 'This is the ID column',`age` INT NULL COMMENT 'This is the age column',\n"
                        + "`name` STRING NULL COMMENT 'This is the name column',\n"
                        + "`score` INT NULL ,\n"
                        + "`gender` TINYINT NULL COMMENT 'This is the gender column',\n"
                        + "`create_time` BIGINT NULL COMMENT 'This is the create_time column'\n"
                        + ") ENGINE=OLAP\n"
                        + " UNIQUE KEY (`id`,`age`)\n"
                        + "COMMENT 'doris test comment'\n"
                        + "DISTRIBUTED BY HASH (`id`,`age`)\n"
                        + " PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"in_memory\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"disable_auto_compaction\" = \"false\"\n"
                        + ")");
    }
}

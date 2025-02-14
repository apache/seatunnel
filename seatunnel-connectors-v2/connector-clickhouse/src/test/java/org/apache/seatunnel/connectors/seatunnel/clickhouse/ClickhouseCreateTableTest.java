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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseCatalogUtil;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClickhouseCreateTableTest {

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

        String createTableSql =
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        "CREATE TABLE IF NOT EXISTS  `${database}`.`${table}` (\n"
                                + "    ${rowtype_primary_key},\n"
                                + "    ${rowtype_fields}\n"
                                + ") ENGINE = MergeTree()\n"
                                + "ORDER BY (${rowtype_primary_key})\n"
                                + "PRIMARY KEY (${rowtype_primary_key})\n"
                                + "SETTINGS\n"
                                + "    index_granularity = 8192;",
                        "test1",
                        "test2",
                        TableSchema.builder()
                                .primaryKey(PrimaryKey.of("", Arrays.asList("id", "age")))
                                .constraintKey(
                                        Arrays.asList(
                                                ConstraintKey.of(
                                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                        "unique_key",
                                                        Collections.singletonList(
                                                                ConstraintKey.ConstraintKeyColumn
                                                                        .of(
                                                                                "name",
                                                                                ConstraintKey
                                                                                        .ColumnSortType
                                                                                        .DESC))),
                                                ConstraintKey.of(
                                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                        "unique_key2",
                                                        Collections.singletonList(
                                                                ConstraintKey.ConstraintKeyColumn
                                                                        .of(
                                                                                "score",
                                                                                ConstraintKey
                                                                                        .ColumnSortType
                                                                                        .ASC)))))
                                .columns(columns)
                                .build(),
                        "clickhouse test table",
                        ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        Assertions.assertEquals(
                createTableSql,
                "CREATE TABLE IF NOT EXISTS  `test1`.`test2` (\n"
                        + "    `id` Int64 ,`age` Int32 COMMENT 'test comment',\n"
                        + "    `name` String ,\n"
                        + "`score` Int32 COMMENT '''N''-N',\n"
                        + "`gender` Int8 ,\n"
                        + "`create_time` Int64 \n"
                        + ") ENGINE = MergeTree()\n"
                        + "ORDER BY (`id`,`age`)\n"
                        + "PRIMARY KEY (`id`,`age`)\n"
                        + "SETTINGS\n"
                        + "    index_granularity = 8192;");
        System.out.println(createTableSql);

        String createTemplate = ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.defaultValue();
        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of(StringUtils.EMPTY, Collections.emptyList()))
                        .constraintKey(Collections.emptyList())
                        .columns(columns)
                        .build();
        TablePath tablePath = TablePath.of("test1.test2");
        SeaTunnelRuntimeException actualSeaTunnelRuntimeException =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                                        createTemplate,
                                        "test1",
                                        "test2",
                                        tableSchema,
                                        "clickhouse test table",
                                        ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key()));

        String primaryKeyHolder = SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder();
        SeaTunnelRuntimeException exceptSeaTunnelRuntimeException =
                CommonError.sqlTemplateHandledError(
                        tablePath.getFullName(),
                        SaveModePlaceHolder.getDisplay(primaryKeyHolder),
                        createTemplate,
                        primaryKeyHolder,
                        ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
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
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                                + "`L_COMMITDATE`,\n"
                                + "${rowtype_primary_key},\n"
                                + "L_SUPPKEY BIGINT NOT NULL,\n"
                                + "${rowtype_fields}\n"
                                + ") ENGINE=MergeTree()\n"
                                + " ORDER BY (L_COMMITDATE, ${rowtype_primary_key}, L_SUPPKEY)\n"
                                + " PRIMARY KEY (L_COMMITDATE, ${rowtype_primary_key}, L_SUPPKEY)\n"
                                + "SETTINGS\n"
                                + "    index_granularity = 8192;",
                        "tpch",
                        "lineitem",
                        TableSchema.builder()
                                .primaryKey(
                                        PrimaryKey.of(
                                                "", Arrays.asList("L_ORDERKEY", "L_LINENUMBER")))
                                .columns(columns)
                                .build(),
                        "clickhouse test table",
                        ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        String expected =
                "CREATE TABLE IF NOT EXISTS `tpch`.`lineitem` (\n"
                        + "`L_COMMITDATE` Date ,\n"
                        + "`L_ORDERKEY` Int32 ,`L_LINENUMBER` Int32 ,\n"
                        + "L_SUPPKEY BIGINT NOT NULL,\n"
                        + "`L_PARTKEY` Int32 ,\n"
                        + "`L_QUANTITY` Decimal(15, 2) ,\n"
                        + "`L_EXTENDEDPRICE` Decimal(15, 2) ,\n"
                        + "`L_DISCOUNT` Decimal(15, 2) ,\n"
                        + "`L_TAX` Decimal(15, 2) ,\n"
                        + "`L_RETURNFLAG` String ,\n"
                        + "`L_LINESTATUS` String ,\n"
                        + "`L_SHIPDATE` Date ,\n"
                        + "`L_RECEIPTDATE` Date ,\n"
                        + "`L_SHIPINSTRUCT` String ,\n"
                        + "`L_SHIPMODE` String ,\n"
                        + "`L_COMMENT` String \n"
                        + ") ENGINE=MergeTree()\n"
                        + " ORDER BY (L_COMMITDATE, `L_ORDERKEY`,`L_LINENUMBER`, L_SUPPKEY)\n"
                        + " PRIMARY KEY (L_COMMITDATE, `L_ORDERKEY`,`L_LINENUMBER`, L_SUPPKEY)\n"
                        + "SETTINGS\n"
                        + "    index_granularity = 8192;";
        Assertions.assertEquals(result, expected);
    }

    @Test
    public void testTableComment() {
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
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                                + "${rowtype_primary_key},\n"
                                + "${rowtype_fields}\n"
                                + ") ENGINE = MergeTree()\n"
                                + "ORDER BY (${rowtype_primary_key})\n"
                                + "PRIMARY KEY (${rowtype_primary_key})\n"
                                + "SETTINGS\n"
                                + "    index_granularity = 8192\n"
                                + "COMMENT '${comment}';",
                        "tpch",
                        "lineitem",
                        TableSchema.builder()
                                .primaryKey(
                                        PrimaryKey.of(
                                                "", Arrays.asList("L_ORDERKEY", "L_LINENUMBER")))
                                .columns(columns)
                                .build(),
                        "clickhouse test table",
                        ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        String expected =
                "CREATE TABLE IF NOT EXISTS `tpch`.`lineitem` (\n"
                        + "`L_ORDERKEY` Int32 ,`L_LINENUMBER` Int32 ,\n"
                        + "`L_PARTKEY` Int32 ,\n"
                        + "`L_SUPPKEY` Int32 ,\n"
                        + "`L_QUANTITY` Decimal(15, 2) ,\n"
                        + "`L_EXTENDEDPRICE` Decimal(15, 2) ,\n"
                        + "`L_DISCOUNT` Decimal(15, 2) ,\n"
                        + "`L_TAX` Decimal(15, 2) ,\n"
                        + "`L_RETURNFLAG` String ,\n"
                        + "`L_LINESTATUS` String ,\n"
                        + "`L_SHIPDATE` Date ,\n"
                        + "`L_COMMITDATE` Date ,\n"
                        + "`L_RECEIPTDATE` Date ,\n"
                        + "`L_SHIPINSTRUCT` String ,\n"
                        + "`L_SHIPMODE` String ,\n"
                        + "`L_COMMENT` String \n"
                        + ") ENGINE = MergeTree()\n"
                        + "ORDER BY (`L_ORDERKEY`,`L_LINENUMBER`)\n"
                        + "PRIMARY KEY (`L_ORDERKEY`,`L_LINENUMBER`)\n"
                        + "SETTINGS\n"
                        + "    index_granularity = 8192\n"
                        + "COMMENT 'clickhouse test table';";
        Assertions.assertEquals(result, expected);
    }
}

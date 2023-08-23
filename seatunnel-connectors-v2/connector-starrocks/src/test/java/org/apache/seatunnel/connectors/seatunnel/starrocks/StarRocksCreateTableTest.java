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

package org.apache.seatunnel.connectors.seatunnel.starrocks;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StarRocksCreateTableTest {

    @Test
    public void test() {

        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("gender", BasicType.BYTE_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("create_time", BasicType.LONG_TYPE, null, true, null, ""));

        String result =
                StarRocksSaveModeUtil.fillingCreateSql(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (                                                                                                                                                   \n"
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
                        "test1",
                        "test2",
                        TableSchema.builder()
                                .primaryKey(PrimaryKey.of("", Arrays.asList("id", "age")))
                                .columns(columns)
                                .build());

        System.out.println(result);
    }

    @Test
    public void testInSeq() {

        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("L_ORDERKEY", BasicType.INT_TYPE, null, false, null, ""));
        columns.add(PhysicalColumn.of("L_PARTKEY", BasicType.INT_TYPE, null, false, null, ""));
        columns.add(PhysicalColumn.of("L_SUPPKEY", BasicType.INT_TYPE, null, false, null, ""));
        columns.add(PhysicalColumn.of("L_LINENUMBER", BasicType.INT_TYPE, null, false, null, ""));
        columns.add(PhysicalColumn.of("L_QUANTITY", new DecimalType(15, 2), null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_EXTENDEDPRICE", new DecimalType(15, 2), null, false, null, ""));
        columns.add(PhysicalColumn.of("L_DISCOUNT", new DecimalType(15, 2), null, false, null, ""));
        columns.add(PhysicalColumn.of("L_TAX", new DecimalType(15, 2), null, false, null, ""));
        columns.add(
                PhysicalColumn.of("L_RETURNFLAG", BasicType.STRING_TYPE, null, false, null, ""));
        columns.add(
                PhysicalColumn.of("L_LINESTATUS", BasicType.STRING_TYPE, null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_SHIPDATE", LocalTimeType.LOCAL_DATE_TYPE, null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_COMMITDATE", LocalTimeType.LOCAL_DATE_TYPE, null, false, null, ""));
        columns.add(
                PhysicalColumn.of(
                        "L_RECEIPTDATE", LocalTimeType.LOCAL_DATE_TYPE, null, false, null, ""));
        columns.add(
                PhysicalColumn.of("L_SHIPINSTRUCT", BasicType.STRING_TYPE, null, false, null, ""));
        columns.add(PhysicalColumn.of("L_SHIPMODE", BasicType.STRING_TYPE, null, false, null, ""));
        columns.add(PhysicalColumn.of("L_COMMENT", BasicType.STRING_TYPE, null, false, null, ""));

        String result =
                StarRocksSaveModeUtil.fillingCreateSql(
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (\n"
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
                        "tpch",
                        "lineitem",
                        TableSchema.builder()
                                .primaryKey(
                                        PrimaryKey.of(
                                                "", Arrays.asList("L_ORDERKEY", "L_LINENUMBER")))
                                .columns(columns)
                                .build());
        String expected =
                "CREATE TABLE IF NOT EXISTS `tpch`.`lineitem` (\n"
                        + "`L_COMMITDATE` DATE NOT NULL ,\n"
                        + "`L_ORDERKEY` INT NOT NULL ,`L_LINENUMBER` INT NOT NULL ,\n"
                        + "L_SUPPKEY BIGINT NOT NULL,\n"
                        + "`L_PARTKEY` INT NOT NULL ,\n"
                        + "`L_QUANTITY` Decimal(15, 2) NOT NULL ,\n"
                        + "`L_EXTENDEDPRICE` Decimal(15, 2) NOT NULL ,\n"
                        + "`L_DISCOUNT` Decimal(15, 2) NOT NULL ,\n"
                        + "`L_TAX` Decimal(15, 2) NOT NULL ,\n"
                        + "`L_RETURNFLAG` STRING NOT NULL ,\n"
                        + "`L_LINESTATUS` STRING NOT NULL ,\n"
                        + "`L_SHIPDATE` DATE NOT NULL ,\n"
                        + "`L_RECEIPTDATE` DATE NOT NULL ,\n"
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
}

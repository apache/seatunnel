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
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;

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
}

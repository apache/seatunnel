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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DorisCatalogUtilTest {

    @Test
    void getCreateTableStatement() {

        String template =
                "CREATE TABLE ${table_identifier}\n"
                        + "(\n"
                        + "${column_definition}\n"
                        + ")\n"
                        + "ENGINE = ${engine_type}\n"
                        + "UNIQUE KEY (${key_columns})\n"
                        + "COMMENT ${table_comment}\n"
                        + "${partition_info}\n"
                        + "DISTRIBUTED BY HASH (${distribution_columns}) BUCKETS ${distribution_bucket}\n"
                        + "PROPERTIES (\n"
                        + "${properties}\n"
                        + ")";

        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("k1", BasicType.INT_TYPE, 10, false, 0, "k1"));
        builder.column(PhysicalColumn.of("k2", BasicType.STRING_TYPE, 64, false, "", "k2"));
        builder.column(PhysicalColumn.of("v1", BasicType.DOUBLE_TYPE, 10, true, null, "v1"));
        builder.column(PhysicalColumn.of("v2", new DecimalType(10, 2), 0, false, 0.1, "v2"));
        builder.primaryKey(PrimaryKey.of("pk", Arrays.asList("k1", "k2")));
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", "test", "create_tbl"),
                        builder.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test");
        Map<String, String> props = new HashMap<>();
        props.put("replication_allocation", "tag.location.default:3");
        props.put("light_schema_change", "true");

        String statement =
                DorisCatalogUtil.getCreateTableStatement(
                        template,
                        TablePath.of("test", "create_test_tbl"),
                        catalogTable,
                        Collections.singletonList("k1"),
                        "AUTO",
                        props);

        String result =
                "CREATE TABLE `test`.`create_test_tbl`\n"
                        + "(\n"
                        + "`k1` INT(11) NOT NULL DEFAULT \"0\" COMMENT \"k1\",\n"
                        + "`k2` VARCHAR(64) NOT NULL DEFAULT \"\" COMMENT \"k2\",\n"
                        + "`v1` DOUBLE NULL COMMENT \"v1\",\n"
                        + "`v2` DECIMALV3(10,2) NOT NULL DEFAULT \"0.1\" COMMENT \"v2\"\n"
                        + ")\n"
                        + "ENGINE = OLAP\n"
                        + "UNIQUE KEY (`k1`,`k2`)\n"
                        + "COMMENT \"test\"\n"
                        + "DISTRIBUTED BY HASH (`k1`) BUCKETS AUTO\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default:3\",\n"
                        + "\"light_schema_change\" = \"true\"\n"
                        + ")";

        Assertions.assertEquals(statement, result);

        TableSchema.Builder builder1 = TableSchema.builder();
        builder1.column(PhysicalColumn.of("k1", BasicType.INT_TYPE, 10, false, 0, "k1"));
        builder1.column(PhysicalColumn.of("k2", BasicType.STRING_TYPE, 64, false, "", "k2"));
        builder1.column(PhysicalColumn.of("v1", BasicType.DOUBLE_TYPE, 10, true, null, "v1"));
        builder1.column(PhysicalColumn.of("v2", new DecimalType(10, 2), 0, false, 0.1, "v2"));
        builder1.primaryKey(PrimaryKey.of("pk", Arrays.asList("k1", "k2")));
        CatalogTable catalogTable1 =
                CatalogTable.of(
                        TableIdentifier.of("doris", "test", "create_tbl1"),
                        builder1.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test");
        Map<String, String> props1 = new HashMap<>();
        props.put("replication_allocation", "tag.location.default:3");
        props.put("light_schema_change", "true");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        DorisCatalogUtil.getCreateTableStatement(
                                template,
                                TablePath.of("test", "create_test_tbl1"),
                                catalogTable1,
                                Collections.singletonList("v1"),
                                "AUTO",
                                props1));
    }
}

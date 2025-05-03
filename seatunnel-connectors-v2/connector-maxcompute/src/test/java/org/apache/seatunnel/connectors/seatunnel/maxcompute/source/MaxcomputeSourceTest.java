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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MaxcomputeSourceTest {

    @Test
    public void testParseSchema() {
        Config fields =
                ConfigFactory.empty()
                        .withValue("id", ConfigValueFactory.fromAnyRef("int"))
                        .withValue("name", ConfigValueFactory.fromAnyRef("string"))
                        .withValue("age", ConfigValueFactory.fromAnyRef("int"));

        Config schema = fields.atKey("fields").atKey("schema");

        Config root =
                schema.withValue("project", ConfigValueFactory.fromAnyRef("project"))
                        .withValue("table_name", ConfigValueFactory.fromAnyRef("test_table"));

        MaxcomputeSource maxcomputeSource = new MaxcomputeSource(ReadonlyConfig.fromConfig(root));

        CatalogTable table = maxcomputeSource.getProducedCatalogTables().get(0);
        Assertions.assertEquals("project.test_table", table.getTablePath().toString());
        SeaTunnelRowType seaTunnelRowType = table.getSeaTunnelRowType();
        Assertions.assertEquals(SqlType.INT, seaTunnelRowType.getFieldType(0).getSqlType());

        Map<String, Object> tableList = new HashMap<>();
        Map<String, Object> schemaMap = new HashMap<>();
        Map<String, Object> fieldsMap = new HashMap<>();
        fieldsMap.put("id", "int");
        fieldsMap.put("name", "string");
        fieldsMap.put("age", "int");
        schemaMap.put("fields", fieldsMap);
        tableList.put("schema", schemaMap);
        tableList.put("table_name", "test_table2");

        root =
                ConfigFactory.empty()
                        .withValue("project", ConfigValueFactory.fromAnyRef("project"))
                        .withValue("accessId", ConfigValueFactory.fromAnyRef("accessId"))
                        .withValue("accesskey", ConfigValueFactory.fromAnyRef("accessKey"))
                        .withValue(
                                "table_list",
                                ConfigValueFactory.fromIterable(
                                        Collections.singletonList(tableList)));

        maxcomputeSource = new MaxcomputeSource(ReadonlyConfig.fromConfig(root));

        table = maxcomputeSource.getProducedCatalogTables().get(0);
        Assertions.assertEquals("project.test_table2", table.getTablePath().toString());
    }
}

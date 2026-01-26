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

package org.apache.seatunnel.connectors.seatunnel.hive.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HiveSourceConfigEmptyFilesTest {

    @Test
    void testBuildCatalogTableFromHiveMetaIncludesPartitionColumnsByDefault() {
        Table table = newPartitionedTable();
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());

        CatalogTable catalogTable = HiveSourceConfig.buildCatalogTableFromHiveMeta(config, table);
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();

        Assertions.assertArrayEquals(
                new String[] {"id", "name", "dt", "region"}, rowType.getFieldNames());
        Assertions.assertEquals(Arrays.asList("dt", "region"), catalogTable.getPartitionKeys());
    }

    @Test
    void testBuildCatalogTableFromHiveMetaCanDisablePartitionColumns() {
        Table table = newPartitionedTable();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("parse_partition_from_path", false);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        CatalogTable catalogTable = HiveSourceConfig.buildCatalogTableFromHiveMeta(config, table);
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();

        Assertions.assertArrayEquals(new String[] {"id", "name"}, rowType.getFieldNames());
        Assertions.assertEquals(Arrays.asList("dt", "region"), catalogTable.getPartitionKeys());
    }

    private static Table newPartitionedTable() {
        Table table = new Table();
        table.setDbName("default");
        table.setTableName("t_partitioned");

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(
                Arrays.asList(
                        new FieldSchema("id", "bigint", null),
                        new FieldSchema("name", "string", null)));
        table.setSd(sd);

        List<FieldSchema> partitionKeys =
                Arrays.asList(
                        new FieldSchema("dt", "string", null),
                        new FieldSchema("region", "int", null));
        table.setPartitionKeys(partitionKeys);
        return table;
    }
}

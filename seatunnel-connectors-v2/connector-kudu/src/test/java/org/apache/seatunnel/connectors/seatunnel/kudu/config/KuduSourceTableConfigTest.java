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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.kudu.catalog.KuduCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KuduSourceTableConfigTest {

    @Test
    void testParseKuduSourceConfigWithRegex() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(KuduBaseOptions.TABLE_NAME.key(), "kudu_source_table_\\d+");
        configMap.put(KuduSourceOptions.FILTER.key(), "id > 10");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<String> tables =
                Arrays.asList("kudu_source_table_1", "kudu_source_table_2", "other_table");
        KuduCatalog kuduCatalog = new FakeKuduCatalog(tables);

        List<KuduSourceTableConfig> result =
                KuduSourceTableConfig.parseKuduSourceConfigWithRegex(config, kuduCatalog);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("kudu_source_table_1", result.get(0).getTablePath().getTableName());
        Assertions.assertEquals("kudu_source_table_2", result.get(1).getTablePath().getTableName());
        Assertions.assertEquals("id > 10", result.get(0).getFilter());
        Assertions.assertEquals("id > 10", result.get(1).getFilter());
    }

    private static class FakeKuduCatalog extends KuduCatalog {

        private final List<String> tables;

        FakeKuduCatalog(List<String> tables) {
            super("test_catalog", createCommonConfig());
            this.tables = tables;
        }

        @Override
        public String getDefaultDatabase() {
            return "default_database";
        }

        @Override
        public List<String> listTables(String databaseName) {
            return tables;
        }

        @Override
        public CatalogTable getTable(TablePath tablePath) {
            TableIdentifier identifier = TableIdentifier.of(name(), tablePath);
            TableSchema schema = TableSchema.builder().build();
            return CatalogTable.of(
                    identifier, schema, Collections.emptyMap(), Collections.emptyList(), null);
        }

        private static CommonConfig createCommonConfig() {
            Map<String, Object> map = new HashMap<>();
            map.put(KuduBaseOptions.MASTER.key(), "dummy:7051");
            ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(map);
            return new CommonConfig(readonlyConfig);
        }
    }
}

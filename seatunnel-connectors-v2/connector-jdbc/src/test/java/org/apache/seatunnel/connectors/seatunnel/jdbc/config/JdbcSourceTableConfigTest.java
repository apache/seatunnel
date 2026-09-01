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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSourceTableConfigTest {

    @Test
    public void testQueryTableMetadataMergeDefaultsToComment() {
        Map<String, Object> config = new HashMap<>();
        config.put("query", "select * from t1");

        List<JdbcSourceTableConfig> tables =
                JdbcSourceTableConfig.of(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals(
                QueryTableMetadataMergeMode.COMMENT, tables.get(0).getQueryTableMetadataMerge());
    }

    @Test
    public void testQueryTableMetadataMergeGlobalOptionCopiedToTables() {
        Map<String, Object> config = new HashMap<>();
        config.put("query", "select * from t1");
        // Lowercase value exercises the case-insensitive enum conversion of the flat option
        config.put("query_table_metadata_merge", "all");

        List<JdbcSourceTableConfig> tables =
                JdbcSourceTableConfig.of(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(
                QueryTableMetadataMergeMode.ALL, tables.get(0).getQueryTableMetadataMerge());
    }

    @Test
    public void testQueryTableMetadataMergePerTableOverridesGlobal() {
        Map<String, Object> tableWithOverride = new HashMap<>();
        tableWithOverride.put("query", "select * from t1");
        // Lowercase value exercises the @JsonCreator parsing of table_list entries
        tableWithOverride.put("query_table_metadata_merge", "none");
        Map<String, Object> tableWithoutOverride = new HashMap<>();
        tableWithoutOverride.put("table_path", "db.t2");

        Map<String, Object> config = new HashMap<>();
        config.put("table_list", Arrays.asList(tableWithOverride, tableWithoutOverride));
        config.put("query_table_metadata_merge", "all");

        List<JdbcSourceTableConfig> tables =
                JdbcSourceTableConfig.of(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(2, tables.size());
        Assertions.assertEquals(
                QueryTableMetadataMergeMode.NONE, tables.get(0).getQueryTableMetadataMerge());
        Assertions.assertEquals(
                QueryTableMetadataMergeMode.ALL, tables.get(1).getQueryTableMetadataMerge());
    }
}

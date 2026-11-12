/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisTableConfigTest {

    @Test
    public void testDuplicateTableListRejected() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "table_list",
                Arrays.asList(tableEntry("test_db", "table1"), tableEntry("test_db", "table1")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> DorisTableConfig.of(config));

        Assertions.assertTrue(exception.getMessage().contains("not allow null/duplicate"));
    }

    @Test
    public void testDistinctTableListReturnsTwoConfigs() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "table_list",
                Arrays.asList(tableEntry("test_db", "table1"), tableEntry("test_db", "table2")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("test_db.table1", result.get(0).getTableIdentifier());
        Assertions.assertEquals("test_db.table2", result.get(1).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseInsensitiveLowercasesIdentifiers() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        options.put("case_sensitive", false);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("testdb.testtable", result.get(0).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseSensitivePreservesIdentifiers() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        options.put("case_sensitive", true);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("TestDB.TestTable", result.get(0).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseSensitiveAbsentPreservesIdentifiers() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("TestDB.TestTable", result.get(0).getTableIdentifier());
    }

    private static Map<String, Object> tableEntry(String database, String table) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("database", database);
        entry.put("table", table);
        return entry;
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TiDBSourceFactoryTest {

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new TiDBSourceFactory()).optionRule());
    }

    @Test
    public void getTableFullNamesShouldPreferTableNamesOption() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("table-names", "db1.table1, db1.table2, db1.table1");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<String> tableFullNames = TiDBSourceOptions.getTableFullNames(config);

        Assertions.assertEquals(2, tableFullNames.size());
        Assertions.assertEquals("db1.table1", tableFullNames.get(0));
        Assertions.assertEquals("db1.table2", tableFullNames.get(1));
    }

    @Test
    public void getTableFullNamesShouldFallBackToSingleTableOptions() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("database-name", "db1");
        configMap.put("table-name", "table1");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<String> tableFullNames = TiDBSourceOptions.getTableFullNames(config);

        Assertions.assertEquals(1, tableFullNames.size());
        Assertions.assertEquals("db1.table1", tableFullNames.get(0));
    }

    @Test
    public void getTableFullNamesShouldFailWhenNoTableConfigured() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> TiDBSourceOptions.getTableFullNames(config));
    }

    @Test
    public void getTableFullNamesShouldFailOnMalformedTableName() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("table-names", "missing_separator");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> TiDBSourceOptions.getTableFullNames(config));
    }

    @Test
    public void parseShouldSplitOnFirstDot() {
        Assertions.assertEquals("db1", TiDBSourceOptions.parseDatabaseName("db1.table.name"));
        Assertions.assertEquals("table.name", TiDBSourceOptions.parseTableName("db1.table.name"));
        Assertions.assertEquals("db1", TiDBSourceOptions.parseDatabaseName("db1.table1"));
        Assertions.assertEquals("table1", TiDBSourceOptions.parseTableName("db1.table1"));
    }

    @Test
    public void parseShouldFailOnInvalidFullName() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> TiDBSourceOptions.parseDatabaseName("db1table"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> TiDBSourceOptions.parseTableName(".db1"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> TiDBSourceOptions.parseTableName("db1."));
    }
}

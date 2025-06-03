/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSourceTest {

    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "password";

    @Test
    public void testExactTableMatch() {
        // Create source config with exact table path
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", DRIVER_CLASS);
        configMap.put("url", URL);
        configMap.put("user", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table_path", "test.table1");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        config, Thread.currentThread().getContextClassLoader());

        // Create source
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));

        // Verify table configuration
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();
        Assertions.assertEquals(1, catalogTables.size());
        Assertions.assertEquals("table1", catalogTables.get(0).getTableId().getTableName());
    }

    @Test
    public void testRegexTableMatch() {
        // Create source config with regex pattern
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", DRIVER_CLASS);
        configMap.put("url", URL);
        configMap.put("user", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table_path", "test.table+");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        config, Thread.currentThread().getContextClassLoader());

        // Create source
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));

        // Verify table configuration
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();
        Assertions.assertTrue(catalogTables.size() > 0);
        for (CatalogTable catalogTable : catalogTables) {
            String tableName = catalogTable.getTableId().getTableName();
            Assertions.assertTrue(tableName.matches("table\\d+"));
        }
    }
}

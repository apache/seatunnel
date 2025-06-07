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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Disabled("Please Test it in your local environment")
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

        // 使用正则表达式匹配test库中的table1和table2
        // 格式：database.table_pattern，其中database可以是具体名称或正则表达式
        configMap.put("table_path", "test.table+");  // 匹配test数据库中table后跟单个数字的表
        configMap.put("use_regex", true);

        // 明确指定MySQL方言
        configMap.put("dialect", "mysql");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // 创建源
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));

        // 验证表配置
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();

        // 验证找到了两个表
        Assertions.assertEquals(2, catalogTables.size(),
            "Should find exactly 2 tables (table1 and table2)");

        // 验证表名是table1和table2
        Set<String> tableNames = catalogTables.stream()
            .map(table -> table.getTableId().getTableName())
            .collect(Collectors.toSet());

        Assertions.assertTrue(tableNames.contains("table1"),
            "Should find table1");
        Assertions.assertTrue(tableNames.contains("table2"),
            "Should find table2");

        // 测试另一个不匹配任何表的正则表达式
        Map<String, Object> noMatchConfigMap = new HashMap<>(configMap);
        noMatchConfigMap.put("table_path", "test.nonexistent.*");

        ReadonlyConfig noMatchConfig = ReadonlyConfig.fromMap(noMatchConfigMap);
        JdbcSource noMatchJdbcSource = new JdbcSource(JdbcSourceConfig.of(noMatchConfig));

        List<CatalogTable> noMatchCatalogTables = noMatchJdbcSource.getProducedCatalogTables();
        Assertions.assertEquals(0, noMatchCatalogTables.size(),
            "Should not find any tables with non-existent pattern");
    }

    @Test
    public void testRegexTableMatchWithEscapedDots() {
        // Test regex patterns with escaped dots to ensure proper handling
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", DRIVER_CLASS);
        configMap.put("url", URL);
        configMap.put("user", USERNAME);
        configMap.put("password", PASSWORD);

        // 使用包含转义点的正则表达式
        configMap.put("table_path", "test\\.table\\d");  // 匹配test.table后跟单个数字的表
        configMap.put("use_regex", true);
        configMap.put("dialect", "mysql");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // 这应该不会抛出异常，并且能正确处理转义的点
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();

        // 验证能够正确处理包含转义点的正则表达式
        Assertions.assertNotNull(catalogTables, "Should handle escaped dots in regex patterns");
    }
}

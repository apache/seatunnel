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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class JdbcSourceFactoryTest {

    private final JdbcSourceFactory factory = new JdbcSourceFactory();
    private final OptionRule rule = factory.optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/test");
        cfg.put("driver", "com.mysql.cj.jdbc.Driver");
        return cfg;
    }

    @Test
    void testValidConfigWithTablePath() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("table_path", "test.users");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidConfigWithTableList() {
        Map<String, Object> cfg = baseConfig();
        List<Map<String, String>> tableList = new ArrayList<>();
        Map<String, String> entry = new HashMap<>();
        entry.put("table_path", "test.users");
        tableList.add(entry);
        cfg.put("table_list", tableList);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidConfigWithQuery() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("query", "SELECT * FROM users");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidConfigWithTablePathAndQuery() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("table_path", "test.users");
        cfg.put("query", "SELECT * FROM users WHERE active = 1");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidConfigWithWhereCondition() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("table_path", "test.users");
        cfg.put("where_condition", "where id > 100");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testTableListWithTablePathFails() {
        Map<String, Object> cfg = baseConfig();
        List<Map<String, String>> tableList = new ArrayList<>();
        Map<String, String> entry = new HashMap<>();
        entry.put("table_path", "test.users");
        tableList.add(entry);
        cfg.put("table_list", tableList);
        cfg.put("table_path", "test.orders");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableListWithQueryFails() {
        Map<String, Object> cfg = baseConfig();
        List<Map<String, String>> tableList = new ArrayList<>();
        Map<String, String> entry = new HashMap<>();
        entry.put("table_path", "test.users");
        tableList.add(entry);
        cfg.put("table_list", tableList);
        cfg.put("query", "SELECT * FROM orders");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableListWithBothTablePathAndQueryFails() {
        Map<String, Object> cfg = baseConfig();
        List<Map<String, String>> tableList = new ArrayList<>();
        Map<String, String> entry = new HashMap<>();
        entry.put("table_path", "test.users");
        tableList.add(entry);
        cfg.put("table_list", tableList);
        cfg.put("table_path", "test.orders");
        cfg.put("query", "SELECT * FROM orders");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testWhereConditionWithoutPrefixFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("table_path", "test.users");
        cfg.put("where_condition", "id > 100");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingUrlFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("driver", "com.mysql.cj.jdbc.Driver");
        cfg.put("table_path", "test.users");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingDriverFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/test");
        cfg.put("table_path", "test.users");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    // ---- Entry-level regression tests through factory-context path ----

    /**
     * Simulates the FactoryUtil.createAndPrepareSource entry path: OptionRule validation followed
     * by factory.createSource(context). Verifies the real submission-time path end-to-end.
     */
    @Test
    void testFactoryContextPathValidConfig() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("table_path", "test.users");
        ReadonlyConfig config = ReadonlyConfig.fromMap(cfg);
        ConfigValidator.of(config).validate(factory.optionRule());

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(config, getClass().getClassLoader());
        Assertions.assertDoesNotThrow(() -> factory.createSource(context));
    }

    @Test
    void testFactoryContextPathTableListExclusionFails() {
        Map<String, Object> cfg = baseConfig();
        List<Map<String, String>> tableList = new ArrayList<>();
        Map<String, String> entry = new HashMap<>();
        entry.put("table_path", "test.users");
        tableList.add(entry);
        cfg.put("table_list", tableList);
        cfg.put("table_path", "test.orders");

        ReadonlyConfig config = ReadonlyConfig.fromMap(cfg);
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(config).validate(factory.optionRule()));
    }
}

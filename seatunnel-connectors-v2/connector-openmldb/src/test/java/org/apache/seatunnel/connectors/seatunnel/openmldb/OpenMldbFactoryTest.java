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

package org.apache.seatunnel.connectors.seatunnel.openmldb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.openmldb.source.OpenMldbSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class OpenMldbFactoryTest {

    private final OptionRule optionRule = new OpenMldbSourceFactory().optionRule();

    @Test
    void testMultiTableConfigAccepted() {
        Map<String, Object> config = requiredConfig(false);
        config.remove("sql");
        config.put(
                "tables_configs",
                Arrays.asList(
                        table("orders", "select * from orders"),
                        table("customers", "select * from customers")));
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    private Map<String, Object> table(String name, String sql) {
        Map<String, Object> table = new HashMap<>();
        table.put("sql", sql);
        Map<String, Object> schema = new HashMap<>();
        schema.put("table", name);
        schema.put("fields", Collections.singletonMap("id", "STRING"));
        table.put("schema", schema);
        return table;
    }

    @Test
    void testSqlAndTablesAreMutuallyExclusive() {
        Map<String, Object> config = requiredConfig(false);
        config.put(
                "tables_configs",
                Collections.singletonList(table("orders", "select * from orders")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testEmptyTablesRejected() {
        Map<String, Object> config = multiConfig();
        config.put("tables_configs", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testDuplicateTableIdentityRejected() {
        Map<String, Object> config = multiConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        table("orders", "select * from orders"),
                        table("orders", "select * from archived")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testEntrySqlRequiredAndNotBlank() {
        for (String sql : new String[] {null, "", " \t\n"}) {
            Map<String, Object> config = multiConfig();
            config.put("tables_configs", Collections.singletonList(table("orders", sql)));
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @Test
    void testSchemaFieldsRequired() {
        for (Object fields : new Object[] {null, Collections.emptyMap(), "id STRING"}) {
            Map<String, Object> config = multiConfig();
            Map<String, Object> entry = table("orders", "select * from orders");
            ((Map<String, Object>) entry.get("schema")).put("fields", fields);
            config.put("tables_configs", Collections.singletonList(entry));
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @Test
    void testTableIdentityRequired() {
        for (String tableId : new String[] {null, "", " \t"}) {
            Map<String, Object> config = multiConfig();
            config.put(
                    "tables_configs",
                    Collections.singletonList(table(tableId, "select * from orders")));
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @Test
    void testRootSchemaRejectedInMultiTableMode() {
        Map<String, Object> config = multiConfig();
        config.put("schema", table("root", "select * from orders").get("schema"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testEntryConnectionOverridesRejected() {
        for (String key :
                new String[] {"host", "port", "cluster_mode", "zk_host", "request_timeout"}) {
            Map<String, Object> config = multiConfig();
            Map<String, Object> entry = table("orders", "select * from orders");
            entry.put(key, "override");
            config.put("tables_configs", Collections.singletonList(entry));
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @Test
    void testEntryDatabaseOverrideValidated() {
        Map<String, Object> config = multiConfig();
        Map<String, Object> entry = table("orders", "select * from orders");
        config.put("tables_configs", Collections.singletonList(entry));
        entry.put("database", "another_db");
        Assertions.assertDoesNotThrow(() -> validate(config));
        entry.put("database", " ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private Map<String, Object> multiConfig() {
        Map<String, Object> config = requiredConfig(false);
        config.remove("sql");
        config.put(
                "tables_configs",
                Collections.singletonList(table("orders", "select * from orders")));
        return config;
    }

    @Test
    void testUnsupportedFieldTypesRejectedBeforeConnecting() {
        for (String type : new String[] {"DECIMAL(10,2)", "TINYINT", "ARRAY<INT>", "BYTES"}) {
            Map<String, Object> config = multiConfig();
            Map<String, Object> entry = table("orders", "select * from orders");
            ((Map<String, Object>) entry.get("schema"))
                    .put("fields", Collections.singletonMap("id", type));
            config.put("tables_configs", Collections.singletonList(entry));
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNonblankSqlAccepted(boolean clusterMode) {
        for (String sql :
                new String[] {
                    "select * from test_table", " select * from test_table ", "not parsed here"
                }) {
            Map<String, Object> config = requiredConfig(clusterMode);
            config.put("sql", sql);
            Assertions.assertDoesNotThrow(() -> validate(config));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMissingSqlRejected(boolean clusterMode) {
        Map<String, Object> config = requiredConfig(clusterMode);
        config.remove("sql");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptySqlRejected(boolean clusterMode) {
        assertInvalidSql(clusterMode, "");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWhitespaceOnlySqlRejected(boolean clusterMode) {
        assertInvalidSql(clusterMode, " ");
        assertInvalidSql(clusterMode, "\t\r\n");
    }

    private void assertInvalidSql(boolean clusterMode, String sql) {
        Map<String, Object> config = requiredConfig(clusterMode);
        config.put("sql", sql);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "OpenMldb");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> requiredConfig(boolean clusterMode) {
        Map<String, Object> config = new HashMap<>();
        config.put("cluster_mode", clusterMode);
        config.put("database", "test_db");
        config.put("sql", "select * from test_table");
        if (clusterMode) {
            config.put("zk_host", "localhost:2181");
            config.put("zk_path", "/openmldb");
        } else {
            config.put("host", "localhost");
            config.put("port", 6527);
        }
        return config;
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new OpenMldbSourceFactory()).optionRule());
    }
}

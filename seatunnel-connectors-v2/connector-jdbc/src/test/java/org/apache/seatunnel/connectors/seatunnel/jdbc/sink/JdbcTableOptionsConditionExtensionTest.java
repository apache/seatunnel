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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies {@code table_options} early validation is wired through {@link
 * JdbcSinkFactory#optionRule()} and {@link JdbcTableOptionsConditionExtension}. Dialect-specific
 * allowlists are covered in {@code *DialectTest} classes.
 */
class JdbcTableOptionsConditionExtensionTest {

    @Test
    void testMysqlTableOptionsPassViaOptionRule() {
        Map<String, Object> config = mysqlSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        tableOptions.put("charset", "utf8mb4");
        tableOptions.put("collate", "utf8mb4_unicode_ci");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testPostgresTableOptionsPassViaOptionRule() {
        Map<String, Object> config = postgresSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "pg_default");
        tableOptions.put("fillfactor", "70");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testPostgresRejectsUnknownTableOptionsViaOptionRule() {
        Map<String, Object> config = postgresSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Postgres"));
    }

    @Test
    void testAbsentTableOptionsSkipsExtension() {
        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(mysqlSinkConfig()));
    }

    @Test
    void testEmptyTableOptionsSkipsExtension() {
        Map<String, Object> config = mysqlSinkConfig();
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), new HashMap<>());

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testTidbTableOptionsPassViaOptionRule() {
        Map<String, Object> config = tidbSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        tableOptions.put("charset", "utf8mb4");
        tableOptions.put("collate", "utf8mb4_unicode_ci");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testOceanBaseMysqlTableOptionsPassViaOptionRule() {
        Map<String, Object> config = oceanBaseMysqlSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        tableOptions.put("charset", "utf8mb4");
        tableOptions.put("collate", "utf8mb4_unicode_ci");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testOracleTableOptionsPassViaOptionRule() {
        Map<String, Object> config = oracleSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "USERS");
        tableOptions.put("pctfree", "10");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testOracleRejectsUnknownTableOptionsViaOptionRule() {
        Map<String, Object> config = oracleSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Oracle"));
    }

    @Test
    void testOceanBaseOracleTableOptionsPassViaOptionRule() {
        Map<String, Object> config = oceanBaseOracleSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "USERS");
        tableOptions.put("pctfree", "10");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testOceanBaseOracleRejectsUnknownTableOptionsViaOptionRule() {
        Map<String, Object> config = oceanBaseOracleSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Oracle"));
    }

    @Test
    void testDamengTableOptionsPassViaOptionRule() {
        Map<String, Object> config = damengSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "MAIN");
        tableOptions.put("fillfactor", "80");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testDamengRejectsUnknownTableOptionsViaOptionRule() {
        Map<String, Object> config = damengSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Dameng"));
    }

    @Test
    void testKingbaseTableOptionsPassViaOptionRule() {
        Map<String, Object> config = kingbaseSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "pg_default");
        tableOptions.put("fillfactor", "70");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testKingbaseRejectsUnknownTableOptionsViaOptionRule() {
        Map<String, Object> config = kingbaseSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("KingBase"));
    }

    private static void validateSinkOptionRule(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new JdbcSinkFactory().optionRule());
    }

    private static Map<String, Object> mysqlSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://127.0.0.1:3306/test");
        config.put("driver", "com.mysql.cj.jdbc.Driver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> postgresSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:postgresql://127.0.0.1:5432/test");
        config.put("driver", "org.postgresql.Driver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> tidbSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://127.0.0.1:4000/test");
        config.put("driver", "com.mysql.cj.jdbc.Driver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> oracleSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:oracle:thin:@127.0.0.1:1521:ORCL");
        config.put("driver", "oracle.jdbc.OracleDriver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> oceanBaseMysqlSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:oceanbase://127.0.0.1:2881/test");
        config.put("driver", "com.oceanbase.jdbc.Driver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> oceanBaseOracleSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:oceanbase://127.0.0.1:2881/test");
        config.put("driver", "com.oceanbase.jdbc.Driver");
        config.put("compatible_mode", "oracle");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> damengSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:dm://127.0.0.1:5236");
        config.put("driver", "dm.jdbc.driver.DmDriver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }

    private static Map<String, Object> kingbaseSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:kingbase8://127.0.0.1:54321/test");
        config.put("driver", "com.kingbase8.Driver");
        config.put("query", "INSERT INTO test_table VALUES (?)");
        return config;
    }
}

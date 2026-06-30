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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase.OceanBaseCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.saphana.SapHanaCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalogFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JdbcCatalogFactoryTest {

    private final OptionRule mysqlRule = new MySqlCatalogFactory().optionRule();
    private final OptionRule pgRule = new PostgresCatalogFactory().optionRule();

    private void validate(OptionRule rule, Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    // ==================== Standard URL validators (MySQL / Postgres) ====================

    @Test
    void testValidCatalogConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        cfg.put("username", "root");
        cfg.put("password", "secret");
        Assertions.assertDoesNotThrow(() -> validate(mysqlRule, cfg));
    }

    @Test
    void testMySqlCatalogValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://host:3306/mydb");
        cfg.put("username", "root");
        cfg.put("password", "pass");
        Assertions.assertDoesNotThrow(() -> validate(mysqlRule, cfg));
    }

    @Test
    void testPostgresCatalogValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:postgresql://host:5432/mydb");
        cfg.put("username", "postgres");
        cfg.put("password", "pass");
        Assertions.assertDoesNotThrow(() -> validate(pgRule, cfg));
    }

    @Test
    void testBlankUrlFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "");
        cfg.put("username", "root");
        cfg.put("password", "pass");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(mysqlRule, cfg));
    }

    @Test
    void testMissingCredentialsFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(mysqlRule, cfg));
    }

    @Test
    void testMissingPasswordFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        cfg.put("username", "root");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(mysqlRule, cfg));
    }

    @Test
    void testCatalogConfigMimicsExtractCatalogConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        cfg.put("username", "root");
        cfg.put("password", "pass");
        cfg.put("decimal_type_narrowing", true);
        cfg.put("handle_blob_as_string", false);
        Assertions.assertDoesNotThrow(() -> validate(mysqlRule, cfg));
    }

    // ==================== OceanBase ====================

    @Test
    void testOceanBaseWithoutCompatibleModeFails() {
        OptionRule obRule = new OceanBaseCatalogFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oceanbase://host:2881/mydb");
        cfg.put("username", "root");
        cfg.put("password", "pass");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(obRule, cfg));
    }

    @Test
    void testOceanBaseValidConfig() {
        OptionRule obRule = new OceanBaseCatalogFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oceanbase://localhost:2881/mydb");
        cfg.put("username", "root");
        cfg.put("password", "pass");
        cfg.put("compatible_mode", "mysql");
        Assertions.assertDoesNotThrow(() -> validate(obRule, cfg));
    }

    // ==================== Dameng (no database in URL) ====================

    @Test
    void testDamengCatalogUrlWithoutDatabase() {
        OptionRule dmRule = new DamengCatalogFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:dm://e2e_dmdb:5236");
        cfg.put("username", "SYSDBA");
        cfg.put("password", "SYSDBA");
        Assertions.assertDoesNotThrow(() -> validate(dmRule, cfg));
    }

    // ==================== DuckDB (no credentials required) ====================

    @Test
    void testDuckDBCatalogConfigNoCredentials() {
        OptionRule rule = new DuckDBCatalogFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:duckdb:/tmp/test.db");
        Assertions.assertDoesNotThrow(() -> validate(rule, cfg));
    }

    // ==================== SqlServer dialect validator ====================

    @Test
    void testSqlServerCatalogUrlWithDatabaseNameProperty() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sqlserver://localhost:1433;databaseName=seatunnel");
        cfg.put("username", "sa");
        cfg.put("password", "Password!");
        Assertions.assertDoesNotThrow(
                () -> validate(new SqlServerCatalogFactory().optionRule(), cfg));
    }

    @Test
    void testSqlServerCatalogUrlWithoutDatabasePasses() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sqlserver://localhost:1433;encrypt=false");
        cfg.put("username", "sa");
        cfg.put("password", "Password!");
        Assertions.assertDoesNotThrow(
                () -> validate(new SqlServerCatalogFactory().optionRule(), cfg));
    }

    // ==================== Oracle dialect validator ====================

    @Test
    void testOracleCatalogThinUrlWithoutDoubleSlash() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oracle:thin:@localhost:1521/ORCLCDB");
        cfg.put("username", "system");
        cfg.put("password", "oracle");
        Assertions.assertDoesNotThrow(() -> validate(new OracleCatalogFactory().optionRule(), cfg));
    }

    // ==================== SapHana dialect validator ====================

    @Test
    void testSapHanaCatalogHostOnlyUrl() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sap://localhost:39017");
        cfg.put("username", "SYSTEM");
        cfg.put("password", "Password1");
        Assertions.assertDoesNotThrow(
                () -> validate(new SapHanaCatalogFactory().optionRule(), cfg));
    }

    @Test
    void testSapHanaCatalogWithDatabaseParam() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sap://localhost:39017/?databaseName=HXE");
        cfg.put("username", "SYSTEM");
        cfg.put("password", "Password1");
        Assertions.assertDoesNotThrow(
                () -> validate(new SapHanaCatalogFactory().optionRule(), cfg));
    }
}

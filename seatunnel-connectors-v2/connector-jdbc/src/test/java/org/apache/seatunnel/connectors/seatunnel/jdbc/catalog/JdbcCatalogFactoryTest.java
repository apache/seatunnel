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
    void testUrlWithoutDatabaseFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://host:3306");
        cfg.put("username", "root");
        cfg.put("password", "pass");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(mysqlRule, cfg));
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
    void testCatalogConfigWithUrlOnly() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        Assertions.assertDoesNotThrow(() -> validate(mysqlRule, cfg));
    }

    @Test
    void testCatalogConfigWithUsernameOnly() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        cfg.put("username", "root");
        Assertions.assertDoesNotThrow(() -> validate(mysqlRule, cfg));
    }

    @Test
    void testCatalogConfigMimicsExtractCatalogConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:mysql://localhost:3306/mydb");
        cfg.put("username", "root");
        cfg.put("decimal_type_narrowing", true);
        cfg.put("handle_blob_as_string", false);
        Assertions.assertDoesNotThrow(() -> validate(mysqlRule, cfg));
    }

    @Test
    void testPostgresCatalogConfigWithoutPassword() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:postgresql://localhost:5432/mydb");
        cfg.put("username", "postgres");
        Assertions.assertDoesNotThrow(() -> validate(pgRule, cfg));
    }

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
    void testOceanBaseCatalogConfigWithoutPassword() {
        OptionRule obRule = new OceanBaseCatalogFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oceanbase://localhost:2881/mydb");
        cfg.put("username", "root");
        cfg.put("compatible_mode", "mysql");
        Assertions.assertDoesNotThrow(() -> validate(obRule, cfg));
    }

    @Test
    void testDuckDBCatalogConfigNoCredentials() {
        OptionRule rule = new DuckDBCatalogFactory().optionRule();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:duckdb:/tmp/test.db");
        Assertions.assertDoesNotThrow(() -> validate(rule, cfg));
    }

    /**
     * SQLServer catalog validation must understand the databaseName property syntax used by the
     * JDBC driver and the CDC E2E configs.
     */
    @Test
    void testSqlServerCatalogUrlWithDatabaseNameProperty() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sqlserver://localhost:1433;databaseName=seatunnel");
        cfg.put("username", "sa");
        cfg.put("password", "Password!");
        Assertions.assertDoesNotThrow(
                () -> validate(new SqlServerCatalogFactory().optionRule(), cfg));
    }

    /** SQLServer catalog validation should still reject URLs that omit the target database. */
    @Test
    void testSqlServerCatalogUrlWithoutDatabaseFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sqlserver://localhost:1433;encrypt=false");
        cfg.put("username", "sa");
        cfg.put("password", "Password!");
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validate(new SqlServerCatalogFactory().optionRule(), cfg));
    }

    /**
     * Oracle thin URLs may omit the double-slash segment, so catalog validation must not rely on
     * the generic host:port/database parser.
     */
    @Test
    void testOracleCatalogThinUrlWithoutDoubleSlash() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oracle:thin:@localhost:1521/ORCLCDB");
        cfg.put("username", "system");
        cfg.put("password", "oracle");
        Assertions.assertDoesNotThrow(() -> validate(new OracleCatalogFactory().optionRule(), cfg));
    }

    /**
     * SAP HANA catalog discovery uses the fixed SYSTEM database even when the JDBC URL only
     * contains the host and port.
     */
    @Test
    void testSapHanaCatalogHostOnlyUrl() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:sap://localhost:39017");
        cfg.put("username", "SYSTEM");
        cfg.put("password", "Password1");
        Assertions.assertDoesNotThrow(
                () -> validate(new SapHanaCatalogFactory().optionRule(), cfg));
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.FactoryUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class SqlServerIncrementalSourceFactoryTest {

    private final OptionRule rule = new SqlServerIncrementalSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validSqlServerConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("username", "sa");
        cfg.put("password", "pass");
        cfg.put("url", "jdbc:sqlserver://localhost:1433;databaseName=test");
        cfg.put("database-names", Collections.singletonList("test"));
        cfg.put("table-names", Collections.singletonList("dbo.users"));
        return cfg;
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(rule);
        Assertions.assertDoesNotThrow(() -> validate(validSqlServerConfig()));
    }

    @Test
    public void testNumericConstraints() {
        Map<String, Object> cfg = validSqlServerConfig();
        cfg.put("connect.timeout.ms", 30000L);
        cfg.put("connect.max-retries", 3);
        cfg.put("connection.pool.size", 20);
        Assertions.assertDoesNotThrow(() -> validate(cfg));

        Map<String, Object> cfgTimeoutNeg = validSqlServerConfig();
        cfgTimeoutNeg.put("connect.timeout.ms", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgTimeoutNeg));

        Map<String, Object> cfgPoolZero = validSqlServerConfig();
        cfgPoolZero.put("connection.pool.size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgPoolZero));

        Map<String, Object> cfgSamplingZero = validSqlServerConfig();
        cfgSamplingZero.put("inverse-sampling.rate", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgSamplingZero));
    }

    @Test
    public void testTableNamesNotEmpty() {
        Map<String, Object> cfgEmpty = validSqlServerConfig();
        cfgEmpty.put("table-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validSqlServerConfig();
        cfgValid.put("table-names", Arrays.asList("dbo.t1", "dbo.t2"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }

    @Test
    public void testTableNamesFormatExtension() {
        Map<String, Object> cfgInvalid = validSqlServerConfig();
        cfgInvalid.put("table-names", Arrays.asList("users"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgInvalid));
    }

    @Test
    public void testDatabaseNamesRequired() {
        Map<String, Object> cfgMissing = validSqlServerConfig();
        cfgMissing.remove("database-names");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgMissing));

        Map<String, Object> cfgEmpty = validSqlServerConfig();
        cfgEmpty.put("database-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validSqlServerConfig();
        cfgValid.put("database-names", Collections.singletonList("testdb"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }

    @Test
    public void testSchemaChangesValidNamesAccepted() {
        Map<String, Object> cfg = validSqlServerConfig();
        cfg.put("schema-changes.enabled", true);
        cfg.put("schema-changes.include", Arrays.asList("add.column", "drop.column"));
        cfg.put("schema-changes.exclude", Arrays.asList("modify.column"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testSchemaChangesInvalidNameFailsFast() {
        Map<String, Object> cfg = validSqlServerConfig();
        cfg.put("schema-changes.include", Arrays.asList("rename.tabble"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testSchemaChangesInvalidExcludeNameFailsFast() {
        Map<String, Object> cfg = validSqlServerConfig();
        cfg.put("schema-changes.exclude", Arrays.asList("create.table"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    /**
     * SQLServer CDC source creation must accept the driver-specific databaseName URL syntax during
     * the submission-time catalog validation step.
     */
    @Test
    public void testCreateOptionalCatalogWithSqlServerStyleUrl() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:sqlserver://localhost:1433;databaseName=seatunnel");
        config.put("username", "sa");
        config.put("password", "Password!");
        config.put("database-names", Arrays.asList("seatunnel"));
        config.put("table-names", Arrays.asList("seatunnel.dbo.orders"));

        Optional<Catalog> catalog =
                FactoryUtil.createOptionalCatalog(
                        "SqlServer",
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader(),
                        "SqlServer");

        Assertions.assertTrue(catalog.isPresent());
        catalog.ifPresent(Catalog::close);
    }
}

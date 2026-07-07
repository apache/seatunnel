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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class OracleIncrementalSourceFactoryTest {

    private final OptionRule rule = new OracleIncrementalSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validOracleConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("username", "system");
        cfg.put("password", "oracle");
        cfg.put("database-names", Collections.singletonList("ORCL"));
        cfg.put("table-names", Collections.singletonList("SCHEMA1.TABLE1"));
        cfg.put("hostname", "localhost");
        cfg.put("port", 1521);
        return cfg;
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(rule);
        Assertions.assertDoesNotThrow(() -> validate(validOracleConfig()));
    }

    @Test
    public void testNumericConstraints() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("connect.timeout.ms", 30000L);
        cfg.put("connect.max-retries", 3);
        cfg.put("connection.pool.size", 20);
        cfg.put("sample-sharding.threshold", 1000);
        cfg.put("inverse-sampling.rate", 1000);
        Assertions.assertDoesNotThrow(() -> validate(cfg));

        Map<String, Object> cfgTimeoutNeg = validOracleConfig();
        cfgTimeoutNeg.put("connect.timeout.ms", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgTimeoutNeg));

        Map<String, Object> cfgPoolZero = validOracleConfig();
        cfgPoolZero.put("connection.pool.size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgPoolZero));

        Map<String, Object> cfgSamplingZero = validOracleConfig();
        cfgSamplingZero.put("inverse-sampling.rate", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgSamplingZero));
    }

    @Test
    public void testTableNamesNotEmpty() {
        Map<String, Object> cfgEmpty = validOracleConfig();
        cfgEmpty.put("table-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validOracleConfig();
        cfgValid.put("table-names", Arrays.asList("S1.T1", "S1.T2"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }

    @Test
    public void testTableNamesFormatExtension() {
        Map<String, Object> cfgInvalid = validOracleConfig();
        cfgInvalid.put("table-names", Arrays.asList("TABLE_ONLY"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgInvalid));

        Map<String, Object> cfgValidThreeSegment = validOracleConfig();
        cfgValidThreeSegment.put("table-names", Arrays.asList("DB1.S1.T1"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValidThreeSegment));
    }

    @Test
    public void testSchemaChangeLogMiningExtension() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("schema-changes.enabled", true);
        Map<String, String> debezium = new HashMap<>();
        debezium.put("log.mining.strategy", "online_catalog");
        cfg.put("debezium", debezium);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testDebeziumSchemaChangeBypassesSeaTunnelFlag() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("schema-changes.enabled", false);
        Map<String, String> debezium = new HashMap<>();
        debezium.put("include.schema.changes", "true");
        debezium.put("log.mining.strategy", "online_catalog");
        cfg.put("debezium", debezium);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));

        Map<String, Object> cfgOk = validOracleConfig();
        cfgOk.put("schema-changes.enabled", false);
        Map<String, String> debeziumOk = new HashMap<>();
        debeziumOk.put("include.schema.changes", "true");
        debeziumOk.put("log.mining.strategy", "redo_log_catalog");
        cfgOk.put("debezium", debeziumOk);
        Assertions.assertDoesNotThrow(() -> validate(cfgOk));
    }

    @Test
    public void testDatabaseNamesRequired() {
        Map<String, Object> cfgMissing = validOracleConfig();
        cfgMissing.remove("database-names");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgMissing));

        Map<String, Object> cfgEmpty = validOracleConfig();
        cfgEmpty.put("database-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validOracleConfig();
        cfgValid.put("database-names", Collections.singletonList("ORCL"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }

    @Test
    public void testEndpointRequired() {
        Map<String, Object> cfgUrlOnly = validOracleConfig();
        cfgUrlOnly.remove("hostname");
        cfgUrlOnly.remove("port");
        cfgUrlOnly.put("url", "jdbc:oracle:thin:@localhost:1521:ORCL");
        Assertions.assertDoesNotThrow(() -> validate(cfgUrlOnly));

        Map<String, Object> cfgHostPortOnly = validOracleConfig();
        Assertions.assertDoesNotThrow(() -> validate(cfgHostPortOnly));

        Map<String, Object> cfgBoth = validOracleConfig();
        cfgBoth.put("url", "jdbc:oracle:thin:@localhost:1521:ORCL");
        Assertions.assertDoesNotThrow(() -> validate(cfgBoth));

        Map<String, Object> cfgNeither = validOracleConfig();
        cfgNeither.remove("hostname");
        cfgNeither.remove("port");
        cfgNeither.remove("url");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgNeither));
    }

    @Test
    public void testHostnameBundledWithPort() {
        Map<String, Object> cfgHostNoPort = validOracleConfig();
        cfgHostNoPort.remove("port");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgHostNoPort));
    }

    @Test
    public void testSchemaChangesValidNamesAccepted() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("schema-changes.enabled", true);
        cfg.put("schema-changes.include", Arrays.asList("add.column", "drop.column"));
        cfg.put("schema-changes.exclude", Arrays.asList("modify.column"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testSchemaChangesInvalidNameFailsFast() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("schema-changes.include", Arrays.asList("rename.tabble"));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("schema-changes.include"));
    }

    @Test
    public void testSchemaChangesInvalidExcludeNameFailsFast() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("schema-changes.exclude", Arrays.asList("create.table"));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("schema-changes.exclude"));
    }

    // ==================== startup.mode / stop.mode validators ====================

    @Test
    public void testStartupModeTimestampValid() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("startup.mode", "TIMESTAMP");
        cfg.put("startup.timestamp", 1000L);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testStartupModeTimestampMissingTimestampFails() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("startup.mode", "TIMESTAMP");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeTimestampNegativeFails() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("startup.mode", "TIMESTAMP");
        cfg.put("startup.timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeInitialLatestPass() {
        for (String mode : Arrays.asList("INITIAL", "LATEST")) {
            Map<String, Object> cfg = validOracleConfig();
            cfg.put("startup.mode", mode);
            Assertions.assertDoesNotThrow(() -> validate(cfg));
        }
    }

    @Test
    public void testStartupModeSpecificRejectedBySingleChoice() {
        // Oracle startup.mode does not allow SPECIFIC.
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("startup.mode", "SPECIFIC");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificValid() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.file", "redo001.log");
        cfg.put("stop.specific-offset.pos", 200L);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificMissingFileFails() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.pos", 200L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificMissingPosFails() {
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.file", "redo001.log");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeNeverLatestPass() {
        for (String mode : Arrays.asList("NEVER", "LATEST")) {
            Map<String, Object> cfg = validOracleConfig();
            cfg.put("stop.mode", mode);
            Assertions.assertDoesNotThrow(() -> validate(cfg));
        }
    }

    @Test
    public void testStopModeTimestampRejectedBySingleChoice() {
        // Oracle stop.mode does not allow TIMESTAMP.
        Map<String, Object> cfg = validOracleConfig();
        cfg.put("stop.mode", "TIMESTAMP");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

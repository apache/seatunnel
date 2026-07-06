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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

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

public class MySqlIncrementalSourceFactoryTest {

    private final OptionRule rule = new MySqlIncrementalSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validMySqlConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("username", "root");
        cfg.put("password", "pass");
        cfg.put("url", "jdbc:mysql://localhost:3306/test");
        cfg.put("table-names", Collections.singletonList("db1.table1"));
        return cfg;
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testServerIdFormatValidation() {
        Assertions.assertDoesNotThrow(() -> validate(validMySqlConfig()));

        Map<String, Object> cfgSingle = validMySqlConfig();
        cfgSingle.put("server-id", "5400");
        Assertions.assertDoesNotThrow(() -> validate(cfgSingle));

        Map<String, Object> cfgRange = validMySqlConfig();
        cfgRange.put("server-id", "5400-5408");
        Assertions.assertDoesNotThrow(() -> validate(cfgRange));

        Map<String, Object> cfgAlpha = validMySqlConfig();
        cfgAlpha.put("server-id", "abc");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgAlpha));

        Map<String, Object> cfgTrail = validMySqlConfig();
        cfgTrail.put("server-id", "5400-");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgTrail));

        Map<String, Object> cfgLead = validMySqlConfig();
        cfgLead.put("server-id", "-5400");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgLead));

        Map<String, Object> cfgMulti = validMySqlConfig();
        cfgMulti.put("server-id", "5400-5408-5500");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgMulti));

        Map<String, Object> cfgDot = validMySqlConfig();
        cfgDot.put("server-id", "54.00");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgDot));
    }

    @Test
    public void testNumericConstraints() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("connect.timeout.ms", 30000L);
        cfg.put("connect.max-retries", 3);
        cfg.put("connection.pool.size", 20);
        cfg.put("sample-sharding.threshold", 1000);
        cfg.put("inverse-sampling.rate", 1000);
        Assertions.assertDoesNotThrow(() -> validate(cfg));

        Map<String, Object> cfgTimeoutZero = validMySqlConfig();
        cfgTimeoutZero.put("connect.timeout.ms", 0L);
        Assertions.assertDoesNotThrow(() -> validate(cfgTimeoutZero));

        Map<String, Object> cfgTimeoutNeg = validMySqlConfig();
        cfgTimeoutNeg.put("connect.timeout.ms", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgTimeoutNeg));

        Map<String, Object> cfgRetriesNeg = validMySqlConfig();
        cfgRetriesNeg.put("connect.max-retries", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgRetriesNeg));

        Map<String, Object> cfgPoolZero = validMySqlConfig();
        cfgPoolZero.put("connection.pool.size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgPoolZero));

        Map<String, Object> cfgSamplingZero = validMySqlConfig();
        cfgSamplingZero.put("inverse-sampling.rate", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgSamplingZero));
    }

    @Test
    public void testTableNamesNotEmpty() {
        Map<String, Object> cfgEmpty = validMySqlConfig();
        cfgEmpty.put("table-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validMySqlConfig();
        cfgValid.put("table-names", Arrays.asList("db1.t1", "db1.t2"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }

    @Test
    public void testThreeSegmentTableNameRejected() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("table-names", Arrays.asList("db.schema.table"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testSingleSegmentTableNameRejected() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("table-names", Arrays.asList("table_only"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testSchemaChangesValidNamesAccepted() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("schema-changes.enabled", true);
        cfg.put("schema-changes.include", Arrays.asList("add.column", "drop.column"));
        cfg.put("schema-changes.exclude", Arrays.asList("modify.column"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testSchemaChangesInvalidNameFailsFast() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("schema-changes.include", Arrays.asList("rename.tabble"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testSchemaChangesInvalidExcludeNameFailsFast() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("schema-changes.exclude", Arrays.asList("create.table"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    // ==================== startup.mode / stop.mode validators ====================

    @Test
    public void testStartupModeTimestampValid() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "TIMESTAMP");
        cfg.put("startup.timestamp", 1000L);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testStartupModeTimestampMissingTimestampFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "TIMESTAMP");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeTimestampNegativeFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "TIMESTAMP");
        cfg.put("startup.timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeSpecificValid() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "SPECIFIC");
        cfg.put("startup.specific-offset.file", "mysql-bin.000003");
        cfg.put("startup.specific-offset.pos", 100L);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testStartupModeSpecificMissingFileFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "SPECIFIC");
        cfg.put("startup.specific-offset.pos", 100L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeSpecificBlankFileFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "SPECIFIC");
        cfg.put("startup.specific-offset.file", "  ");
        cfg.put("startup.specific-offset.pos", 100L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeSpecificMissingPosFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "SPECIFIC");
        cfg.put("startup.specific-offset.file", "mysql-bin.000003");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeSpecificNegativePosFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "SPECIFIC");
        cfg.put("startup.specific-offset.file", "mysql-bin.000003");
        cfg.put("startup.specific-offset.pos", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStartupModeInitialEarliestLatestPass() {
        for (String mode : Arrays.asList("INITIAL", "EARLIEST", "LATEST")) {
            Map<String, Object> cfg = validMySqlConfig();
            cfg.put("startup.mode", mode);
            Assertions.assertDoesNotThrow(() -> validate(cfg));
        }
    }

    @Test
    public void testStartupModeInvalidValueFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("startup.mode", "BOGUS");
        Assertions.assertThrows(IllegalArgumentException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificValid() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.file", "mysql-bin.000005");
        cfg.put("stop.specific-offset.pos", 200L);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificMissingFileFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.pos", 200L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificMissingPosFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.file", "mysql-bin.000005");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeSpecificNegativePosFails() {
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("stop.mode", "SPECIFIC");
        cfg.put("stop.specific-offset.file", "mysql-bin.000005");
        cfg.put("stop.specific-offset.pos", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    public void testStopModeLatestNeverPass() {
        for (String mode : Arrays.asList("LATEST", "NEVER")) {
            Map<String, Object> cfg = validMySqlConfig();
            cfg.put("stop.mode", mode);
            Assertions.assertDoesNotThrow(() -> validate(cfg));
        }
    }

    @Test
    public void testStopModeTimestampRejectedBySingleChoice() {
        // MySQL stop.mode does not allow TIMESTAMP (only LATEST, SPECIFIC, NEVER).
        Map<String, Object> cfg = validMySqlConfig();
        cfg.put("stop.mode", "TIMESTAMP");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

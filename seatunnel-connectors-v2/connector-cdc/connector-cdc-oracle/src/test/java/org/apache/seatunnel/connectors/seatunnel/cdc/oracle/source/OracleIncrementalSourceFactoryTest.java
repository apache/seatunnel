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
}

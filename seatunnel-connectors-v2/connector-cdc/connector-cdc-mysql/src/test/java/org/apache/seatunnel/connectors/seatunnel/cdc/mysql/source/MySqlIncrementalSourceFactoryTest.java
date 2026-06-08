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
}

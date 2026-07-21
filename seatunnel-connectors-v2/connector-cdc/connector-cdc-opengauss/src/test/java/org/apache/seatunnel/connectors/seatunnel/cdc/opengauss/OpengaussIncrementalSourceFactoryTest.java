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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss;

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

class OpengaussIncrementalSourceFactoryTest {

    private final OptionRule rule = new OpengaussIncrementalSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("username", "postgres");
        cfg.put("password", "pass");
        cfg.put("url", "jdbc:postgresql://localhost:5432/test");
        cfg.put("database-names", Collections.singletonList("test"));
        cfg.put("table-names", Collections.singletonList("public.users"));
        return cfg;
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(rule);
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    public void testNumericConstraints() {
        Map<String, Object> cfg = validConfig();
        cfg.put("connect.timeout.ms", 30000L);
        cfg.put("connect.max-retries", 3);
        cfg.put("connection.pool.size", 20);
        Assertions.assertDoesNotThrow(() -> validate(cfg));

        Map<String, Object> cfgTimeoutNeg = validConfig();
        cfgTimeoutNeg.put("connect.timeout.ms", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgTimeoutNeg));

        Map<String, Object> cfgPoolZero = validConfig();
        cfgPoolZero.put("connection.pool.size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgPoolZero));

        Map<String, Object> cfgSamplingZero = validConfig();
        cfgSamplingZero.put("inverse-sampling.rate", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgSamplingZero));
    }

    @Test
    public void testTableNamesNotEmpty() {
        Map<String, Object> cfgEmpty = validConfig();
        cfgEmpty.put("table-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validConfig();
        cfgValid.put("table-names", Arrays.asList("public.t1", "public.t2"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }

    @Test
    public void testTableNamesFormatExtension() {
        Map<String, Object> cfgInvalid = validConfig();
        cfgInvalid.put("table-names", Arrays.asList("users"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgInvalid));

        Map<String, Object> cfgValidThreeSegment = validConfig();
        cfgValidThreeSegment.put("table-names", Arrays.asList("db1.public.users"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValidThreeSegment));
    }

    @Test
    public void testDatabaseNamesRequired() {
        Map<String, Object> cfgMissing = validConfig();
        cfgMissing.remove("database-names");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgMissing));

        Map<String, Object> cfgEmpty = validConfig();
        cfgEmpty.put("database-names", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgEmpty));

        Map<String, Object> cfgValid = validConfig();
        cfgValid.put("database-names", Collections.singletonList("mydb"));
        Assertions.assertDoesNotThrow(() -> validate(cfgValid));
    }
}

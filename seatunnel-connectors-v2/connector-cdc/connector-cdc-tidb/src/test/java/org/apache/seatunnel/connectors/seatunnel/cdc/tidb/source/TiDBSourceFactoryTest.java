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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TiDBSourceFactoryTest {
    private final OptionRule rule = new TiDBSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("database-name", "testdb");
        cfg.put("table-name", "users");
        cfg.put("pd-addresses", "127.0.0.1:2379");
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
        cfg.put("tikv.batch.get.concurrency", 2);
        cfg.put("tikv.batch.scan.concurrency", 2);
        cfg.put("tikv.grpc.timeout", 1000L);
        cfg.put("tikv.grpc.scan.timeout", 1000L);
        cfg.put("batch-size-per-scan", 1000);
        Assertions.assertDoesNotThrow(() -> validate(cfg));

        Map<String, Object> invalid = validConfig();
        invalid.put("batch-size-per-scan", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(invalid));
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RocketMqFactoryTest {

    private final OptionRule sourceRule = new RocketMqSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sourceRule);
    }

    private Map<String, Object> validTimestampConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        cfg.put("topics", "test-topic");
        cfg.put("start.mode", "CONSUME_FROM_TIMESTAMP");
        cfg.put("start.mode.timestamp", 1000L);
        return cfg;
    }

    private Map<String, Object> validSpecificOffsetsConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        cfg.put("topics", "test-topic");
        cfg.put("start.mode", "CONSUME_FROM_SPECIFIC_OFFSETS");
        Map<String, Long> offsets = new HashMap<>();
        offsets.put("test-topic-0", 100L);
        cfg.put("start.mode.offsets", offsets);
        return cfg;
    }

    private Map<String, Object> validMultiTableConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        List<Map<String, Object>> tableConfigs = new ArrayList<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("topics", "topic-a,topic-b");
        tableConfigs.add(entry);
        cfg.put("tables_configs", tableConfigs);
        return cfg;
    }

    @Test
    void testValidTimestampConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validTimestampConfig()));
    }

    @Test
    void testNegativeTimestampRejected() {
        Map<String, Object> cfg = validTimestampConfig();
        cfg.put("start.mode.timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testValidSpecificOffsetsConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validSpecificOffsetsConfig()));
    }

    @Test
    void testEmptyOffsetsMapRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        cfg.put("topics", "test-topic");
        cfg.put("start.mode", "CONSUME_FROM_SPECIFIC_OFFSETS");
        cfg.put("start.mode.offsets", Collections.emptyMap());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMultiTableValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validMultiTableConfig()));
    }

    @Test
    void testMultiTableMissingTopicsRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        List<Map<String, Object>> tableConfigs = new ArrayList<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("topics", "");
        tableConfigs.add(entry);
        cfg.put("tables_configs", tableConfigs);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMultiTableTimestampModeWithoutTimestampRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        List<Map<String, Object>> tableConfigs = new ArrayList<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("topics", "topic-a");
        entry.put("start.mode", "CONSUME_FROM_TIMESTAMP");
        tableConfigs.add(entry);
        cfg.put("tables_configs", tableConfigs);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMultiTableNegativeTimestampRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        List<Map<String, Object>> tableConfigs = new ArrayList<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("topics", "topic-a");
        entry.put("start.mode", "CONSUME_FROM_TIMESTAMP");
        entry.put("start.mode.timestamp", -5L);
        tableConfigs.add(entry);
        cfg.put("tables_configs", tableConfigs);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMultiTableEmptyOffsetsRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("name.srv.addr", "localhost:9876");
        List<Map<String, Object>> tableConfigs = new ArrayList<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("topics", "topic-a");
        entry.put("start.mode", "CONSUME_FROM_SPECIFIC_OFFSETS");
        entry.put("start.mode.offsets", Collections.emptyMap());
        tableConfigs.add(entry);
        cfg.put("tables_configs", tableConfigs);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

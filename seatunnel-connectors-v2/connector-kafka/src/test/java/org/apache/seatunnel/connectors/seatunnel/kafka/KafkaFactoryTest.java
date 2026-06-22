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

package org.apache.seatunnel.connectors.seatunnel.kafka;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.kafka.sink.KafkaSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KafkaFactoryTest {

    private final OptionRule sourceRule = new KafkaSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sourceRule);
    }

    private Map<String, Object> validTimestampConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "localhost:9092");
        cfg.put("topic", "test-topic");
        cfg.put("start_mode", "TIMESTAMP");
        cfg.put("start_mode.timestamp", 1000L);
        return cfg;
    }

    private Map<String, Object> validSpecificOffsetsConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "localhost:9092");
        cfg.put("topic", "test-topic");
        cfg.put("start_mode", "SPECIFIC_OFFSETS");
        Map<String, Long> offsets = new HashMap<>();
        offsets.put("test-topic-0", 100L);
        cfg.put("start_mode.offsets", offsets);
        return cfg;
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new KafkaSourceFactory()).optionRule());
        Assertions.assertNotNull((new KafkaSinkFactory()).optionRule());
    }

    @Test
    void testValidTimestampConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validTimestampConfig()));
    }

    @Test
    void testNegativeTimestampRejected() {
        Map<String, Object> cfg = validTimestampConfig();
        cfg.put("start_mode.timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testNegativeEndTimestampRejected() {
        Map<String, Object> cfg = validTimestampConfig();
        cfg.put("start_mode.end_timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testValidSpecificOffsetsConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validSpecificOffsetsConfig()));
    }

    @Test
    void testEmptyOffsetsMapRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "localhost:9092");
        cfg.put("topic", "test-topic");
        cfg.put("start_mode", "SPECIFIC_OFFSETS");
        cfg.put("start_mode.offsets", Collections.emptyMap());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    // --- Multi-table (tables_configs) tests ---

    private Map<String, Object> validMultiTableConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "localhost:9092");

        Map<String, Object> entry = new HashMap<>();
        entry.put("topic", "multi-topic");
        entry.put("start_mode", "TIMESTAMP");
        entry.put("start_mode.timestamp", 5000L);
        List<Map<String, Object>> tables = new ArrayList<>();
        tables.add(entry);
        cfg.put("tables_configs", tables);
        return cfg;
    }

    @Test
    void testMultiTableValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validMultiTableConfig()));
    }

    @Test
    void testMultiTableNegativeTimestampRejected() {
        Map<String, Object> cfg = validMultiTableConfig();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tables = (List<Map<String, Object>>) cfg.get("tables_configs");
        tables.get(0).put("start_mode.timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMultiTableNegativeEndTimestampRejected() {
        Map<String, Object> cfg = validMultiTableConfig();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tables = (List<Map<String, Object>>) cfg.get("tables_configs");
        tables.get(0).put("start_mode.end_timestamp", -1L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMultiTableEmptyOffsetsRejected() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "localhost:9092");

        Map<String, Object> entry = new HashMap<>();
        entry.put("topic", "multi-topic");
        entry.put("start_mode", "SPECIFIC_OFFSETS");
        entry.put("start_mode.offsets", Collections.emptyMap());
        List<Map<String, Object>> tables = new ArrayList<>();
        tables.add(entry);
        cfg.put("tables_configs", tables);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

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

package org.apache.seatunnel.transform.jsonpath;

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

class JsonPathTransformFactoryTest {

    private final OptionRule rule = new JsonPathTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> columnEntry(String path, String srcField, String destField) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("path", path);
        entry.put("src_field", srcField);
        entry.put("dest_field", destField);
        return entry;
    }

    @Test
    void testValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Arrays.asList(columnEntry("$.data.name", "raw", "name")));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingColumnsFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyColumnsFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("columns", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithNullPathFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("src_field", "raw");
        entry.put("dest_field", "name");
        cfg.put("columns", Arrays.asList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithEmptyDestFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("path", "$.data.name");
        entry.put("src_field", "raw");
        entry.put("dest_field", "");
        cfg.put("columns", Arrays.asList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithMissingSrcFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("path", "$.data.name");
        entry.put("dest_field", "name");
        cfg.put("columns", Arrays.asList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testColumnWithEmptySrcFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("path", "$.data.name");
        entry.put("src_field", "");
        entry.put("dest_field", "name");
        cfg.put("columns", Arrays.asList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

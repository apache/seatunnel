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

package org.apache.seatunnel.transform.filter;

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

class FilterFieldTransformFactoryTest {

    private final OptionRule rule = new FilterFieldTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testIncludeFieldsValid() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("include_fields", Arrays.asList("id", "name"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testExcludeFieldsValid() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("exclude_fields", Arrays.asList("password", "secret"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testNeitherFieldsFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testBothFieldsFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("include_fields", Arrays.asList("id"));
        cfg.put("exclude_fields", Arrays.asList("password"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testIncludeFieldsEmptyFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("include_fields", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testExcludeFieldsEmptyFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("exclude_fields", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

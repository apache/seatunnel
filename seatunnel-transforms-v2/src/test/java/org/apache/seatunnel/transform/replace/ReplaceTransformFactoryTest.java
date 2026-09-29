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

package org.apache.seatunnel.transform.replace;

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

class ReplaceTransformFactoryTest {

    private final OptionRule rule = new ReplaceTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("replace_fields", Arrays.asList("name"));
        cfg.put("pattern", "old");
        cfg.put("replacement", "new");
        return cfg;
    }

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testMissingReplaceFieldsFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("replace_fields");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyReplaceFieldsFails() {
        Map<String, Object> cfg = validConfig();
        cfg.put("replace_fields", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingPatternFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("pattern");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingReplacementFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("replacement");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testFallbackKeyValid() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("replace_field", "name");
        cfg.put("pattern", "old");
        cfg.put("replacement", "new");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }
}

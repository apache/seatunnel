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

package org.apache.seatunnel.transform.copy;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class CopyFieldTransformFactoryTest {

    private final OptionRule rule = new CopyFieldTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testValidFieldsMapConfig() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("new_col", "old_col");
        cfg.put("fields", fields);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidSrcDestConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("src_field", "old_col");
        cfg.put("dest_field", "new_col");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testEmptyFieldsMapFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("fields", Collections.emptyMap());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testNeitherFieldsNorSrcFieldProvidedFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testSrcFieldWithoutDestFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("src_field", "old_col");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testDestFieldWithoutSrcFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("dest_field", "new_col");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

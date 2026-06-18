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

package org.apache.seatunnel.transform.dynamiccompile;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class DynamicCompileTransformFactoryTest {

    private final OptionRule rule = new DynamicCompileTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testValidSourceCodeConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        cfg.put("compile_pattern", "SOURCE_CODE");
        cfg.put("source_code", "public class Transform {}");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidAbsolutePathConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        cfg.put("compile_pattern", "ABSOLUTE_PATH");
        cfg.put("absolute_path", "/tmp/Transform.java");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingCompileLanguageFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_pattern", "SOURCE_CODE");
        cfg.put("source_code", "public class Transform {}");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testSourceCodeBlankFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        cfg.put("compile_pattern", "SOURCE_CODE");
        cfg.put("source_code", "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testAbsolutePathBlankFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        cfg.put("compile_pattern", "ABSOLUTE_PATH");
        cfg.put("absolute_path", "  ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testSourceCodeMissingKeyFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        cfg.put("compile_pattern", "SOURCE_CODE");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testAbsolutePathMissingKeyFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        cfg.put("compile_pattern", "ABSOLUTE_PATH");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testDefaultPatternMissingSourceCodeFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("compile_language", "JAVA");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}

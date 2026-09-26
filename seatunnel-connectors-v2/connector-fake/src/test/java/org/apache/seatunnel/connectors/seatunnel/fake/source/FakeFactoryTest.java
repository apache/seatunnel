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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeFactoryTest {

    private final FakeSourceFactory factory = new FakeSourceFactory();

    private static Map<String, Object> validBaseConfig() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());
        map.put("schema", schema);
        return map;
    }

    private void validate(Map<String, Object> map) {
        ConfigValidator.of(ReadonlyConfig.fromMap(map)).validate(factory.optionRule());
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(factory.optionRule());
    }

    @Test
    void validConfigPassesValidation() {
        validate(validBaseConfig());
    }

    @Test
    void validNumericBoundsPassValidation() {
        Map<String, Object> map = validBaseConfig();
        map.put("row.num", 1);
        map.put("split.num", 1);
        map.put("split.read-interval", 0);
        map.put("map.size", 0);
        map.put("array.size", 0);
        map.put("bytes.length", 0);
        map.put("string.length", 0);
        map.put("vector.dimension", 1);
        map.put("binary.vector.dimension", 8);
        validate(map);
    }

    @Test
    void rowNumZeroPassesValidation() {
        Map<String, Object> map = validBaseConfig();
        map.put("row.num", 0);
        validate(map);
    }

    @Test
    void minEqualsMaxPassesValidation() {
        Map<String, Object> map = validBaseConfig();
        map.put("int.min", 5);
        map.put("int.max", 5);
        map.put("tinyint.min", 1);
        map.put("tinyint.max", 1);
        map.put("smallint.min", 2);
        map.put("smallint.max", 2);
        map.put("bigint.min", 3L);
        map.put("bigint.max", 3L);
        map.put("float.min", 1.5f);
        map.put("float.max", 1.5f);
        map.put("double.min", 2.5d);
        map.put("double.max", 2.5d);
        map.put("vector.float.min", 0.5f);
        map.put("vector.float.max", 0.5f);
        validate(map);
    }

    @Test
    void oneSidedMinOrMaxPassesValidation() {
        Map<String, Object> onlyMin = validBaseConfig();
        onlyMin.put("int.min", 0);
        validate(onlyMin);

        Map<String, Object> onlyMax = validBaseConfig();
        onlyMax.put("int.max", 100);
        validate(onlyMax);
    }

    @Test
    void exclusiveUsesListForTableConfigs() {
        Map<String, Object> map = validBaseConfig();
        List<Map<String, Object>> tables = Collections.singletonList(new HashMap<>());
        map.put("tables_configs", tables);
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
        String message = ex.getMessage();
        Assertions.assertTrue(
                message.contains("schema") || message.contains("tables_configs"),
                () -> "expected exclusive message, got: " + message);
    }

    @Test
    void neitherSchemaNorTableConfigsRejected() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(new HashMap<>())
        );
    }

    @Test
    void nonPositiveSplitNumIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("split.num", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void negativeMapSizeIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("map.size", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void negativeArraySizeIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("array.size", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void negativeBytesLengthIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("bytes.length", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void negativeStringLengthIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("string.length", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void negativeSplitReadIntervalIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("split.read-interval", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void nonPositiveVectorDimensionIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("vector.dimension", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void nonPositiveBinaryVectorDimensionIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("binary.vector.dimension", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void tinyintMinGreaterThanMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("tinyint.min", 10);
        map.put("tinyint.max", 5);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void smallintMinGreaterThanMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("smallint.min", 10);
        map.put("smallint.max", 5);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void intMinGreaterThanIntMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("int.min", 10);
        map.put("int.max", 5);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void bigintMinGreaterThanMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("bigint.min", 10L);
        map.put("bigint.max", 5L);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void floatMinGreaterThanMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("float.min", 2.0f);
        map.put("float.max", 1.0f);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void doubleMinGreaterThanMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("double.min", 2.0d);
        map.put("double.max", 1.0d);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void vectorFloatMinGreaterThanMaxIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("vector.float.min", 2.0f);
        map.put("vector.float.max", 1.0f);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void templateModeWithoutTemplateIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("string.fake.mode", "TEMPLATE");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void templateModeWithTemplatePassesValidation() {
        Map<String, Object> map = validBaseConfig();
        map.put("string.fake.mode", "TEMPLATE");
        map.put("string.template", Collections.singletonList("hello"));
        validate(map);
    }
}


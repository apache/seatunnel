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

import java.util.HashMap;
import java.util.Map;

public class FakeFactoryTest {

    private final FakeSourceFactory factory = new FakeSourceFactory();

    /** Exclusive requires exactly one of schema / tables_configs. */
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
        map.put("int.min", 0);
        map.put("int.max", 10);
        validate(map);
    }

    @Test
    void exclusiveSchemaAndTableConfigsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("tables_configs", new HashMap<String, Object>());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void neitherSchemaNorTableConfigsRejected() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(new HashMap<>()));
    }

    @Test
    void nonPositiveRowNumIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("row.num", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
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
    void nonPositiveVectorDimensionIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("vector.dimension", 0);
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
    void templateModeWithoutTemplateIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("string.fake.mode", "TEMPLATE");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }
}

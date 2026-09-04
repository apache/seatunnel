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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FakeFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new FakeSourceFactory()).optionRule());
    }

    @Test
    void invalidTinyintMinShouldFailValidation() {
        Map<String, Object> config = new HashMap<>();
        config.put("tinyint.min", 200);
        config.put(ConnectorCommonOptions.SCHEMA.key(), schema());

        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(exception.getRawMessage().contains("tinyint.min"));
    }

    @Test
    void invalidTinyintMinInTablesConfigsShouldFailValidation() {
        Map<String, Object> childConfig = new HashMap<>();
        childConfig.put("tinyint.min", 200);
        childConfig.put(ConnectorCommonOptions.SCHEMA.key(), schema());
        Map<String, Object> config = new HashMap<>();
        config.put(
                ConnectorCommonOptions.TABLE_CONFIGS.key(), Collections.singletonList(childConfig));

        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(exception.getRawMessage().contains("tables_configs[0]"));
        Assertions.assertTrue(exception.getRawMessage().contains("tinyint.min"));
    }

    @Test
    void validTinyintMinInTablesConfigsShouldPassValidation() {
        Map<String, Object> childConfig = new HashMap<>();
        childConfig.put("tinyint.min", 100);
        childConfig.put(ConnectorCommonOptions.SCHEMA.key(), schema());
        Map<String, Object> config = new HashMap<>();
        config.put(
                ConnectorCommonOptions.TABLE_CONFIGS.key(), Collections.singletonList(childConfig));

        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    private static void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new FakeSourceFactory().optionRule());
    }

    private static Map<String, Object> schema() {
        return Collections.singletonMap("table", "fake.table");
    }
}

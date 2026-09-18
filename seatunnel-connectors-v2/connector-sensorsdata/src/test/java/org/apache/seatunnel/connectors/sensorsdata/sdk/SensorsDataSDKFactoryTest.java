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

package org.apache.seatunnel.connectors.sensorsdata.sdk;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.sensorsdata.format.config.TargetColumnConfig;
import org.apache.seatunnel.connectors.sensorsdata.sdk.sink.SensorsDataSDKSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class SensorsDataSDKFactoryTest {

    private final SensorsDataSDKSinkFactory factory = new SensorsDataSDKSinkFactory();

    private static Map<String, Object> validBaseConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("entity_name", "users");
        map.put("record_type", "users");
        map.put("server_url", "http://127.0.0.1:8106/sa?project=default");
        // Conditional on entity_name=users (also the option default) — see
        // SensorsDataBaseOptionRules
        map.put("schema", "users");
        map.put("distinct_id_column", "name");
        map.put(
                "identity_fields",
                Arrays.asList(new TargetColumnConfig("name", "String", "$identity_name")));
        map.put(
                "property_fields",
                Arrays.asList(new TargetColumnConfig("name", "String", "name")));
        return map;
    }

    private void validate(Map<String, Object> map) {
        ConfigValidator.of(ReadonlyConfig.fromMap(ImmutableMap.copyOf(map)))
                .validate(factory.optionRule());
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
    void boundaryValuesPassValidation() {
        Map<String, Object> map = validBaseConfig();
        map.put("bulk_size", 1);
        map.put("max_cache_row_size", 0);
        validate(map);
    }

    @Test
    void missingServerUrlIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.remove("server_url");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
        Assertions.assertTrue(ex.getMessage().contains("server_url"));
    }

    @Test
    void blankServerUrlIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("server_url", "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void nonPositiveBulkSizeIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("bulk_size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }

    @Test
    void negativeMaxCacheRowSizeIsRejected() {
        Map<String, Object> map = validBaseConfig();
        map.put("max_cache_row_size", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(map));
    }
}

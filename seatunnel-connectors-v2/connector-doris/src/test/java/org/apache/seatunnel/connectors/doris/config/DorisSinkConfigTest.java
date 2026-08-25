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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.doris.sink.DorisSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DorisSinkConfigTest {

    @Test
    public void testDirectToBeRequiresBenodes() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("direct_to_be", true);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                ConfigValidator.of(config)
                                        .validate(new DorisSinkFactory().optionRule()));

        Assertions.assertTrue(
                exception.getMessage().contains("benodes"),
                "Error message should mention 'benodes' but was: " + exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("direct_to_be"),
                "Error message should mention 'direct_to_be' but was: " + exception.getMessage());
    }

    @Test
    public void testDirectToBeRejectsBlankBenodes() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("direct_to_be", true);
        configMap.put("benodes", "   ");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                ConfigValidator.of(config)
                                        .validate(new DorisSinkFactory().optionRule()));

        Assertions.assertTrue(
                exception.getMessage().contains("benodes"),
                "Error message should mention 'benodes' but was: " + exception.getMessage());
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("fenodes", "fe-1:8030");
        config.put("username", "root");
        config.put("password", "root");
        config.put("doris.config", new HashMap<String, String>());
        return config;
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.file.gcs;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.config.GcsFileSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.source.GcsFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.source.GcsFileSourceFactory;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsFileSourceFactoryTest {

    @Test
    void shouldIdentifyGcsFileSource() {
        GcsFileSourceFactory factory = new GcsFileSourceFactory();

        assertEquals("GcsFile", factory.factoryIdentifier());
        assertEquals(GcsFileSource.class, factory.getSourceClass());
    }

    @Test
    void shouldRequireOnlyCoreGcsSourceOptionsForBinaryFiles() {
        OptionRule optionRule = new GcsFileSourceFactory().optionRule();
        Map<String, Object> config = sourceConfig();

        assertDoesNotThrow(() -> validate(config, optionRule));

        config.remove(GcsFileSourceOptions.BUCKET.key());
        assertThrows(OptionValidationException.class, () -> validate(config, optionRule));
    }

    @Test
    void shouldExposeAuthenticationAndFileBaseOptions() {
        OptionRule optionRule = new GcsFileSourceFactory().optionRule();

        assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(GcsFileSourceOptions.SERVICE_ACCOUNT_KEY_FILE));
        assertTrue(optionRule.getOptionalOptions().contains(GcsFileSourceOptions.GCS_PROPERTIES));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.DISCOVERY_MODE));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SYNC_MODE));
        assertTrue(optionRuleContains(optionRule, FileBaseSourceOptions.ENABLE_FILE_SPLIT));
        assertTrue(optionRuleContains(optionRule, FileBaseSourceOptions.FILE_SPLIT_SIZE));
    }

    private static Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(GcsFileSourceOptions.FILE_PATH.key(), "/source");
        config.put(GcsFileSourceOptions.FILE_FORMAT_TYPE.key(), "binary");
        config.put(GcsFileSourceOptions.BUCKET.key(), "gs://test-bucket");
        return config;
    }

    private static boolean optionRuleContains(OptionRule optionRule, Option<?> option) {
        if (optionRule.getOptionalOptions().contains(option)) {
            return true;
        }
        return optionRule.getRequiredOptions().stream()
                .anyMatch(requiredOption -> requiredOption.getOptions().contains(option));
    }

    private static void validate(Map<String, Object> config, OptionRule optionRule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(optionRule);
    }
}

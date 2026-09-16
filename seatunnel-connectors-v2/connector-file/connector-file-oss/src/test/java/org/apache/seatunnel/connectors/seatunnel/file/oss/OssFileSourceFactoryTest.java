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

package org.apache.seatunnel.connectors.seatunnel.file.oss;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.oss.source.OssFileSourceFactory;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OssFileSourceFactoryTest {

    @Test
    void shouldExposeContinuousDiscoveryOptions() {
        OptionRule optionRule = new OssFileSourceFactory().optionRule();

        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.DISCOVERY_MODE));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SCAN_INTERVAL));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.START_MODE));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SYNC_MODE));
        assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.TARGET_HADOOP_CONF));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.UPDATE_STRATEGY));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.COMPARE_MODE));
        assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_PARALLELISM));
        assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_BULK_THRESHOLD));
        assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.POST_SYNC_ACTION));
        assertTrue(optionRule.getOptionalOptions().contains(FileBaseSourceOptions.BACKUP_PATH));
        assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.RETENTION_MAX_AGE));
        assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL));
    }

    @Test
    void shouldRequireTargetPathForUpdateMode() {
        OptionRule optionRule = new OssFileSourceFactory().optionRule();
        Map<String, Object> config = sourceConfig();
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");

        assertThrows(OptionValidationException.class, () -> validate(config, optionRule));

        config.put(FileBaseSourceOptions.TARGET_PATH.key(), "/target");
        assertDoesNotThrow(() -> validate(config, optionRule));
    }

    private static Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseOptions.FILE_PATH.key(), "/source");
        return config;
    }

    private static void validate(Map<String, Object> config, OptionRule optionRule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(optionRule);
    }
}

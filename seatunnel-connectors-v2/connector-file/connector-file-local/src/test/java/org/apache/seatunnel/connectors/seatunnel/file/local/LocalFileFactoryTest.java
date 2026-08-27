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

package org.apache.seatunnel.connectors.seatunnel.file.local;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.LocalFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.LocalFileSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class LocalFileFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new LocalFileSinkFactory()).optionRule());
        OptionRule optionRule = (new LocalFileSourceFactory()).optionRule();
        Assertions.assertNotNull(optionRule);
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SYNC_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.TARGET_HADOOP_CONF));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.UPDATE_STRATEGY));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.COMPARE_MODE));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_PARALLELISM));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_BULK_THRESHOLD));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.DISCOVERY_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SCAN_INTERVAL));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.START_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.POST_SYNC_ACTION));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.BACKUP_PATH));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.RETENTION_MAX_AGE));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL));
        Assertions.assertTrue(
                optionRule.getRequiredOptions().stream()
                        .filter(RequiredOption.ConditionalRequiredOptions.class::isInstance)
                        .map(RequiredOption.ConditionalRequiredOptions.class::cast)
                        .anyMatch(
                                required ->
                                        required.getOptions()
                                                .contains(
                                                        FileBaseSourceOptions
                                                                .MARKDOWN_RAG_METADATA_ENABLED)));
    }

    @Test
    void syncUpdateRequiresTargetPath() {
        OptionRule optionRule = (new LocalFileSourceFactory()).optionRule();
        Map<String, Object> config = sourceConfig();
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");

        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(config, optionRule));

        config.put(FileBaseSourceOptions.TARGET_PATH.key(), "/target");
        Assertions.assertDoesNotThrow(() -> validate(config, optionRule));
    }

    @Test
    void postSyncActionValidation() {
        OptionRule optionRule = (new LocalFileSourceFactory()).optionRule();
        Map<String, Object> noneConfig = sourceConfig();
        noneConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "none");
        Assertions.assertDoesNotThrow(() -> validate(noneConfig, optionRule));

        noneConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), "/backup");
        noneConfig.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "7D");
        noneConfig.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "1H");
        Assertions.assertDoesNotThrow(() -> validate(noneConfig, optionRule));

        Map<String, Object> backupConfig = sourceConfig();
        backupConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(backupConfig, optionRule));

        backupConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), "/backup");
        Assertions.assertDoesNotThrow(() -> validate(backupConfig, optionRule));
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

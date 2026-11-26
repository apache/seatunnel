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

package org.apache.seatunnel.connectors.test;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.oss.jindo.sink.OssFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.oss.jindo.source.OssFileSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class OssJindoFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new OssFileSourceFactory()).optionRule());
        Assertions.assertNotNull((new OssFileSinkFactory()).optionRule());
    }

    @Test
    void consumedOptionsAcceptedByValidateUnknownKeys() {
        // These options are consumed by AbstractReadStrategy / CsvReadStrategy /
        // BinaryReadStrategy at runtime. They MUST be declared in the factory's
        // optionRule() so that ConfigValidator.validateUnknownKeys (called by
        // --check / --dry-run at SeaTunnelConfValidateCommand:325) does not
        // reject them as "unknown option keys".
        Map<String, Object> consumedOptions = new LinkedHashMap<>();
        consumedOptions.put(
                FileBaseSourceOptions.READ_PARTITIONS.key(),
                Collections.singletonList("year=2024"));
        consumedOptions.put(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key(), true);
        consumedOptions.put(FileBaseSourceOptions.BINARY_CHUNK_SIZE.key(), 2048);
        consumedOptions.put(FileBaseSourceOptions.BINARY_COMPLETE_FILE_MODE.key(), true);
        consumedOptions.put(
                FileBaseSourceOptions.FILE_FILTER_MODIFIED_START.key(), "2024-01-01 00:00:00");
        consumedOptions.put(
                FileBaseSourceOptions.FILE_FILTER_MODIFIED_END.key(), "2024-12-31 23:59:59");
        consumedOptions.put(FileBaseSourceOptions.SORT_FILES_BY_MOD_TIME.key(), true);
        consumedOptions.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), "gzip");
        consumedOptions.put(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), "zip");

        OptionRule optionRule = (new OssFileSourceFactory()).optionRule();

        for (Map.Entry<String, Object> entry : consumedOptions.entrySet()) {
            Map<String, Object> config = sourceConfig();
            config.put(entry.getKey(), entry.getValue());

            Assertions.assertDoesNotThrow(
                    () ->
                            ConfigValidator.validateUnknownKeys(
                                    ReadonlyConfig.fromMap(config), optionRule, "OssFile"),
                    "validateUnknownKeys should accept declared option: " + entry.getKey());
        }
    }

    private static Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseOptions.FILE_PATH.key(), "/source");
        return config;
    }
}

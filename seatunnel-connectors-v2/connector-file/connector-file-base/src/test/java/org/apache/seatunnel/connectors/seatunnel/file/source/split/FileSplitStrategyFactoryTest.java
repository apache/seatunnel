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
package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class FileSplitStrategyFactoryTest {

    @Test
    void shouldThrowWhenSplitSizeIsNonPositive() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        configMap.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT);
        configMap.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.NONE);
        configMap.put(
                FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), ArchiveCompressFormat.NONE);
        configMap.put(FileBaseSourceOptions.FILE_SPLIT_SIZE.key(), 0L);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        HadoopConf hadoopConf = new HadoopConf("file:///");

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                FileSplitStrategyFactory.initFileSplitStrategy(
                                        readonlyConfig, hadoopConf));
        Assertions.assertEquals(
                FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("file_split_size"));
    }

    @Test
    void shouldFallbackToDefaultWhenCompressed() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        configMap.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT);
        configMap.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.LZO);
        configMap.put(FileBaseSourceOptions.FILE_SPLIT_SIZE.key(), 0L);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);

        FileSplitStrategy strategy =
                FileSplitStrategyFactory.initFileSplitStrategy(readonlyConfig, null);
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, strategy);
    }

    @Test
    void shouldFallbackToDefaultWhenFormatNotSupportSplit() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        configMap.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.ORC);
        configMap.put(FileBaseSourceOptions.FILE_SPLIT_SIZE.key(), 0L);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);

        FileSplitStrategy strategy =
                FileSplitStrategyFactory.initFileSplitStrategy(readonlyConfig, null);
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, strategy);
    }
}

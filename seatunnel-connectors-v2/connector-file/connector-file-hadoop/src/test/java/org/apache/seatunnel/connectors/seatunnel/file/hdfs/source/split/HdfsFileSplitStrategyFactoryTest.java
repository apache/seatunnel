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
package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.split;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.AccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.ParquetFileSplitStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class HdfsFileSplitStrategyFactoryTest {

    @Test
    void testInitFileSplitStrategy() {
        HdfsFileHadoopConfig hadoopConf = new HdfsFileHadoopConfig("file:///");

        Map<String, Object> map = baseConfig(FileFormat.ORC);
        map.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        FileSplitStrategy fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map), hadoopConf);
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);

        Map<String, Object> map1 = baseConfig(FileFormat.TEXT);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map1), hadoopConf);
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);

        Map<String, Object> map2 = baseConfig(FileFormat.TEXT);
        map2.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map2), hadoopConf);
        Assertions.assertInstanceOf(AccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);

        Map<String, Object> map3 = baseConfig(FileFormat.CSV);
        map3.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map3), hadoopConf);
        Assertions.assertInstanceOf(AccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);

        Map<String, Object> map4 = baseConfig(FileFormat.JSON);
        map4.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map4), hadoopConf);
        Assertions.assertInstanceOf(AccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);

        Map<String, Object> map5 = baseConfig(FileFormat.PARQUET);
        map5.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map5), hadoopConf);
        Assertions.assertInstanceOf(ParquetFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);

        Map<String, Object> map6 = baseConfig(FileFormat.PARQUET);
        map6.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        map6.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.LZO);
        map6.put(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), ArchiveCompressFormat.NONE);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map6), hadoopConf);
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
    }

    private Map<String, Object> baseConfig(FileFormat fileFormat) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), fileFormat);
        map.put(HdfsSourceConfigOptions.DEFAULT_FS.key(), "file:///");
        return map;
    }

    private void closeQuietly(FileSplitStrategy strategy) {
        try {
            if (strategy instanceof Closeable) {
                ((Closeable) strategy).close();
                return;
            }
            if (strategy instanceof AutoCloseable) {
                ((AutoCloseable) strategy).close();
            }
        } catch (Exception ignored) {
            // ignore
        }
    }
}

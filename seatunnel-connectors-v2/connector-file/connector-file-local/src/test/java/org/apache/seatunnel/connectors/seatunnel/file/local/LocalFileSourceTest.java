package org.apache.seatunnel.connectors.seatunnel.file.local;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.LocalFileAccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.LocalFileSplitStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.ParquetFileSplitStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LocalFileSourceTest {

    @Test
    void testInitFileSplitStrategy() {
        // test orc
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.ORC);
        map.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        FileSplitStrategy fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map));
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        // test text, no split
        Map<String, Object> map1 = new HashMap<>();
        map1.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map1));
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        // test text, split
        Map<String, Object> map2 = new HashMap<>();
        map2.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT);
        map2.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map2));
        Assertions.assertInstanceOf(
                LocalFileAccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        // test csv, split
        Map<String, Object> map3 = new HashMap<>();
        map3.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.CSV);
        map3.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map3));
        Assertions.assertInstanceOf(
                LocalFileAccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        // test json, split
        Map<String, Object> map4 = new HashMap<>();
        map4.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.JSON);
        map4.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map4));
        Assertions.assertInstanceOf(
                LocalFileAccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        // test parquet, split
        Map<String, Object> map5 = new HashMap<>();
        map5.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET);
        map5.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map5));
        Assertions.assertInstanceOf(ParquetFileSplitStrategy.class, fileSplitStrategy);
        // test compress 1
        Map<String, Object> map6 = new HashMap<>();
        map6.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET);
        map6.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        map6.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.LZO);
        map6.put(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), ArchiveCompressFormat.NONE);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map6));
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        // test compress 2
        Map<String, Object> map7 = new HashMap<>();
        map7.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET);
        map7.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        map7.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.NONE);
        map7.put(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), ArchiveCompressFormat.NONE);
        fileSplitStrategy =
                LocalFileSplitStrategyFactory.initFileSplitStrategy(ReadonlyConfig.fromMap(map7));
        Assertions.assertInstanceOf(ParquetFileSplitStrategy.class, fileSplitStrategy);
    }
}

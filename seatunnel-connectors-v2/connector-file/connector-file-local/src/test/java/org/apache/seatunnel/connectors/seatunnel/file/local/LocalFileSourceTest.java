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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.local.config.LocalFileHadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.config.LocalFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.AccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.ParquetFileSplitStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LocalFileSourceTest {

    private static final String[] MARKDOWN_FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final String[] MARKDOWN_RAG_METADATA_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };

    @TempDir private Path tempDir;

    @Test
    void testInitFileSplitStrategy() {
        // test orc
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.ORC);
        map.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        FileSplitStrategy fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test text, no split
        Map<String, Object> map1 = new HashMap<>();
        map1.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map1), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test text, split
        Map<String, Object> map2 = new HashMap<>();
        map2.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT);
        map2.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map2), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(AccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test csv, split
        Map<String, Object> map3 = new HashMap<>();
        map3.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.CSV);
        map3.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map3), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(AccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test json, split
        Map<String, Object> map4 = new HashMap<>();
        map4.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.JSON);
        map4.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map4), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(AccordingToSplitSizeSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test parquet, split
        Map<String, Object> map5 = new HashMap<>();
        map5.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET);
        map5.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map5), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(ParquetFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test compress 1
        Map<String, Object> map6 = new HashMap<>();
        map6.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET);
        map6.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        map6.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.LZO);
        map6.put(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), ArchiveCompressFormat.NONE);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map6), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(DefaultFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
        // test compress 2
        Map<String, Object> map7 = new HashMap<>();
        map7.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET);
        map7.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        map7.put(FileBaseSourceOptions.COMPRESS_CODEC.key(), CompressFormat.NONE);
        map7.put(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key(), ArchiveCompressFormat.NONE);
        fileSplitStrategy =
                FileSplitStrategyFactory.initFileSplitStrategy(
                        ReadonlyConfig.fromMap(map7), new LocalFileHadoopConf());
        Assertions.assertInstanceOf(ParquetFileSplitStrategy.class, fileSplitStrategy);
        closeQuietly(fileSplitStrategy);
    }

    @Test
    void testMarkdownSourceDiscoversRagMetadataSchema() throws IOException {
        Path markdownFile = tempDir.resolve("doc.md");
        Files.write(markdownFile, Collections.singletonList("# Title"), StandardCharsets.UTF_8);

        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), markdownFile.toUri().toString());
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        map.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), true);

        CatalogTable catalogTable =
                new LocalFileSourceConfig(
                                ReadonlyConfig.fromMap(map),
                                CatalogTableUtil.buildSimpleTextTable())
                        .getCatalogTable();

        Assertions.assertArrayEquals(
                concat(MARKDOWN_FIELD_NAMES, MARKDOWN_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void testContinuousMarkdownSourceDiscoversSchemaWithoutFiles() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), tempDir.toString());
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        map.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");

        CatalogTable catalogTable =
                new LocalFileSourceConfig(
                                ReadonlyConfig.fromMap(map),
                                CatalogTableUtil.buildSimpleTextTable())
                        .getCatalogTable();

        Assertions.assertArrayEquals(
                MARKDOWN_FIELD_NAMES, catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void testContinuousMarkdownSourceDiscoversRagMetadataSchemaWithoutFiles() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), tempDir.toString());
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        map.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        map.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), true);

        CatalogTable catalogTable =
                new LocalFileSourceConfig(
                                ReadonlyConfig.fromMap(map),
                                CatalogTableUtil.buildSimpleTextTable())
                        .getCatalogTable();

        Assertions.assertArrayEquals(
                concat(MARKDOWN_FIELD_NAMES, MARKDOWN_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
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

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }
}

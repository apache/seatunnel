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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.AccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HdfsFileAccordingToSplitSizeSplitStrategyTest {

    @TempDir private Path tempDir;

    @Test
    void testSplitNonExistingFileShouldThrowFileNotFound() throws Exception {
        String fileUri = tempDir.resolve("not_exist.txt").toUri().toString();
        try (AccordingToSplitSizeSplitStrategy strategy =
                new AccordingToSplitSizeSplitStrategy(
                        new HdfsFileHadoopConfig("file:///"), "\n", 0, "UTF-8", 6)) {
            SeaTunnelRuntimeException ex =
                    Assertions.assertThrows(
                            SeaTunnelRuntimeException.class, () -> strategy.split("t", fileUri));
            Assertions.assertEquals(
                    FileConnectorErrorCode.FILE_NOT_FOUND, ex.getSeaTunnelErrorCode());
        }
    }

    @Test
    void testSplitByDelimiterSeek() throws IOException {
        Path filePath = tempDir.resolve("test.txt");
        Files.write(filePath, "abc\nabc\nabc\nabc\nabc\n".getBytes(StandardCharsets.UTF_8));

        String fileUri = filePath.toUri().toString();
        try (AccordingToSplitSizeSplitStrategy strategy =
                new AccordingToSplitSizeSplitStrategy(
                        new HdfsFileHadoopConfig("file:///"), "\n", 0, "UTF-8", 6)) {
            List<FileSourceSplit> splits = strategy.split("t", fileUri);
            Assertions.assertEquals(3, splits.size());

            Assertions.assertEquals(0, splits.get(0).getStart());
            Assertions.assertEquals(8, splits.get(0).getLength());

            Assertions.assertEquals(8, splits.get(1).getStart());
            Assertions.assertEquals(8, splits.get(1).getLength());

            Assertions.assertEquals(16, splits.get(2).getStart());
            Assertions.assertEquals(4, splits.get(2).getLength());
        }
    }

    @Test
    void testSplitWithSkipHeaderLine() throws IOException {
        Path filePath = tempDir.resolve("with_header.txt");
        Files.write(filePath, "header\nabc\nabc\nabc\nabc\n".getBytes(StandardCharsets.UTF_8));

        String fileUri = filePath.toUri().toString();
        try (AccordingToSplitSizeSplitStrategy strategy =
                new AccordingToSplitSizeSplitStrategy(
                        new HdfsFileHadoopConfig("file:///"), "\n", 1, "UTF-8", 6)) {
            List<FileSourceSplit> splits = strategy.split("t", fileUri);
            Assertions.assertEquals(2, splits.size());

            Assertions.assertEquals(7, splits.get(0).getStart());
            Assertions.assertEquals(8, splits.get(0).getLength());

            Assertions.assertEquals(15, splits.get(1).getStart());
            Assertions.assertEquals(8, splits.get(1).getLength());
        }
    }

    @Test
    void testSplitWithCrLfDelimiter() throws IOException {
        Path filePath = tempDir.resolve("crlf.txt");
        Files.write(filePath, "a\r\nb\r\nc\r\n".getBytes(StandardCharsets.UTF_8));

        String fileUri = filePath.toUri().toString();
        try (AccordingToSplitSizeSplitStrategy strategy =
                new AccordingToSplitSizeSplitStrategy(
                        new HdfsFileHadoopConfig("file:///"), "\r\n", 0, "UTF-8", 2)) {
            List<FileSourceSplit> splits = strategy.split("t", fileUri);
            Assertions.assertEquals(3, splits.size());

            Assertions.assertEquals(0, splits.get(0).getStart());
            Assertions.assertEquals(3, splits.get(0).getLength());

            Assertions.assertEquals(3, splits.get(1).getStart());
            Assertions.assertEquals(3, splits.get(1).getLength());

            Assertions.assertEquals(6, splits.get(2).getStart());
            Assertions.assertEquals(3, splits.get(2).getLength());
        }
    }

    @Test
    void testReadBySplitsShouldMatchFullRead() throws Exception {
        Path filePath = tempDir.resolve("read_compare.txt");
        List<String> lines = new ArrayList<>();
        lines.add("header");
        for (int i = 1; i <= 200; i++) {
            lines.add("value-" + i);
        }
        Files.write(filePath, (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));

        String fileUri = filePath.toUri().toString();
        HdfsFileHadoopConfig hadoopConf = new HdfsFileHadoopConfig("file:///");
        String tableId = "t";

        List<String> fullReadResult =
                readByTextStrategy(
                        hadoopConf,
                        fileUri,
                        tableId,
                        Collections.singletonList(new FileSourceSplit(tableId, fileUri)),
                        false,
                        "\n",
                        1);
        Assertions.assertEquals(200, fullReadResult.size());
        Assertions.assertEquals("value-1", fullReadResult.get(0));

        List<FileSourceSplit> splits;
        try (AccordingToSplitSizeSplitStrategy splitStrategy =
                new AccordingToSplitSizeSplitStrategy(hadoopConf, "\n", 1, "UTF-8", 64)) {
            splits = splitStrategy.split(tableId, fileUri);
        }
        Assertions.assertTrue(splits.size() > 1);

        List<String> splitReadResult =
                readByTextStrategy(hadoopConf, fileUri, tableId, splits, true, "\n", 1);
        Assertions.assertEquals(fullReadResult, splitReadResult);
    }

    private static List<String> readByTextStrategy(
            HdfsFileHadoopConfig hadoopConf,
            String fileUri,
            String tableId,
            List<FileSourceSplit> splits,
            boolean enableFileSplit,
            String rowDelimiter,
            long skipHeaderRows)
            throws Exception {
        Config pluginConfig =
                ConfigFactory.empty()
                        .withValue(
                                FileBaseSourceOptions.FILE_PATH.key(),
                                ConfigValueFactory.fromAnyRef(fileUri))
                        .withValue(
                                FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(),
                                ConfigValueFactory.fromAnyRef(enableFileSplit))
                        .withValue(
                                FileBaseSourceOptions.ROW_DELIMITER.key(),
                                ConfigValueFactory.fromAnyRef(rowDelimiter))
                        .withValue(
                                FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key(),
                                ConfigValueFactory.fromAnyRef(skipHeaderRows));

        List<String> results = new ArrayList<>();
        try (TextReadStrategy readStrategy = new TextReadStrategy()) {
            readStrategy.setPluginConfig(pluginConfig);
            readStrategy.init(hadoopConf);
            readStrategy.getFileNamesByPath(fileUri);
            readStrategy.setCatalogTable(CatalogTableUtil.buildSimpleTextTable());

            FirstFieldCollector collector = new FirstFieldCollector(tableId, results);
            for (FileSourceSplit split : splits) {
                readStrategy.read(split, collector);
            }
        }
        return results;
    }

    private static class FirstFieldCollector implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final String tableId;
        private final List<String> rows;

        private FirstFieldCollector(String tableId, List<String> rows) {
            this.tableId = tableId;
            this.rows = rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            Assertions.assertEquals(tableId, record.getTableId());
            Object field = record.getField(0);
            rows.add(field == null ? null : String.valueOf(field));
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}

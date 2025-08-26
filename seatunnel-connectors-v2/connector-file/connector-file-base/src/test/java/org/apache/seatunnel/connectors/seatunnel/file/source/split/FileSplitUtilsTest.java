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

import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

public class FileSplitUtilsTest {

    @Mock private HadoopFileSystemProxy hadoopFileSystemProxy;

    @Mock private FileStatus fileStatus;

    @Mock private FSDataInputStream fsDataInputStream;

    @TempDir java.nio.file.Path tempDir;

    private AutoCloseable closeable;

    @BeforeEach
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    @Test
    public void testSupportsSplitting() {
        // Text-based formats should support splitting
        Assertions.assertTrue(FileSplitUtils.supportsSplitting(FileFormat.CSV));
        Assertions.assertTrue(FileSplitUtils.supportsSplitting(FileFormat.TEXT));
        Assertions.assertTrue(FileSplitUtils.supportsSplitting(FileFormat.JSON));
        Assertions.assertTrue(FileSplitUtils.supportsSplitting(FileFormat.XML));

        // Binary and special formats should not support splitting
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.PARQUET));
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.ORC));
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.EXCEL));
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.BINARY));
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.CANAL_JSON));
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.DEBEZIUM_JSON));
        Assertions.assertFalse(FileSplitUtils.supportsSplitting(FileFormat.MAXWELL_JSON));
    }

    @Test
    public void testEstimateSplitCount() {
        // Test with 100MB file and 64MB split size
        long fileSize = 100L * 1024 * 1024; // 100MB
        int splitSizeMB = 64;
        int expectedSplits = 2; // ceil(100/64) = 2

        Assertions.assertEquals(
                expectedSplits, FileSplitUtils.estimateSplitCount(fileSize, splitSizeMB));

        // Test with exact split size
        fileSize = 64L * 1024 * 1024; // 64MB
        splitSizeMB = 64;
        expectedSplits = 1;

        Assertions.assertEquals(
                expectedSplits, FileSplitUtils.estimateSplitCount(fileSize, splitSizeMB));

        // Test with zero split size
        splitSizeMB = 0;
        expectedSplits = 1;

        Assertions.assertEquals(
                expectedSplits, FileSplitUtils.estimateSplitCount(fileSize, splitSizeMB));
    }

    @Test
    public void testGenerateFileSplits_SmallFile() throws IOException {
        // Test with a small file that doesn't need splitting
        String filePath = "/test/small.csv";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.CSV;
        int splitSizeMB = 10; // 10MB

        // Mock file status for a 1MB file
        long fileSize = 1024 * 1024; // 1MB
        Mockito.when(hadoopFileSystemProxy.getFileStatus(filePath)).thenReturn(fileStatus);
        Mockito.when(fileStatus.getLen()).thenReturn(fileSize);

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should return a single split for small files
        Assertions.assertEquals(1, splits.size());
        FileSourceSplit split = splits.get(0);
        Assertions.assertEquals(tableId, split.getTableId());
        Assertions.assertEquals(filePath, split.getFilePath());
        Assertions.assertTrue(split.isCompleteFile());
        Assertions.assertTrue(split.isFirstSplit());
    }

    @Test
    public void testGenerateFileSplits_NonCSVFormat() throws IOException {
        // Test with non-supported format (should return single split)
        String filePath = "/test/data.parquet";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.PARQUET;
        int splitSizeMB = 10;

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should return a single split for non-supported files
        Assertions.assertEquals(1, splits.size());
        FileSourceSplit split = splits.get(0);
        Assertions.assertEquals(tableId, split.getTableId());
        Assertions.assertEquals(filePath, split.getFilePath());
        Assertions.assertTrue(split.isCompleteFile());
        Assertions.assertTrue(split.isFirstSplit());
    }

    @Test
    public void testGenerateFileSplits_TextFormat() throws IOException {
        // Test with TEXT format that supports splitting
        String filePath = "/test/data.txt";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.TEXT;
        int splitSizeMB = 1; // 1MB splits

        // Mock file status for a 3MB file
        long fileSize = 3 * 1024 * 1024; // 3MB
        Mockito.when(hadoopFileSystemProxy.getFileStatus(filePath)).thenReturn(fileStatus);
        Mockito.when(fileStatus.getLen()).thenReturn(fileSize);

        // Create mock text content with line breaks
        String textContent = createMockTextContent(fileSize);

        Mockito.when(hadoopFileSystemProxy.getInputStream(filePath)).thenReturn(fsDataInputStream);
        Mockito.doNothing().when(fsDataInputStream).seek(Mockito.anyLong());
        Mockito.when(fsDataInputStream.read(any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        invocation -> {
                            byte[] buffer = invocation.getArgument(0);
                            int offset = invocation.getArgument(1);
                            buffer[offset] = '\n';
                            return 1;
                        });

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should create multiple splits for large text files
        Assertions.assertTrue(splits.size() > 1);

        // Verify split properties
        boolean foundFirstSplit = false;
        for (FileSourceSplit split : splits) {
            Assertions.assertEquals(tableId, split.getTableId());
            Assertions.assertEquals(filePath, split.getFilePath());
            Assertions.assertFalse(split.isCompleteFile());

            if (split.isFirstSplit()) {
                Assertions.assertFalse(foundFirstSplit, "Should have only one first split");
                foundFirstSplit = true;
                Assertions.assertEquals(0L, split.getStartOffset());
            }
        }
        Assertions.assertTrue(foundFirstSplit, "Should have one first split");
    }

    @Test
    public void testGenerateFileSplits_JsonFormat() throws IOException {
        // Test with JSON format that supports splitting
        String filePath = "/test/data.json";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.JSON;
        int splitSizeMB = 1; // 1MB splits

        // Mock file status for a 3MB file
        long fileSize = 3 * 1024 * 1024; // 3MB
        Mockito.when(hadoopFileSystemProxy.getFileStatus(filePath)).thenReturn(fileStatus);
        Mockito.when(fileStatus.getLen()).thenReturn(fileSize);

        Mockito.when(hadoopFileSystemProxy.getInputStream(filePath)).thenReturn(fsDataInputStream);
        Mockito.doNothing().when(fsDataInputStream).seek(Mockito.anyLong());
        Mockito.when(fsDataInputStream.read(any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        invocation -> {
                            byte[] buffer = invocation.getArgument(0);
                            int offset = invocation.getArgument(1);
                            buffer[offset] = '\n';
                            return 1;
                        });

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should create multiple splits for large JSON files
        Assertions.assertTrue(splits.size() > 1);

        // Verify that at least one split is marked as first
        boolean foundFirstSplit = splits.stream().anyMatch(FileSourceSplit::isFirstSplit);
        Assertions.assertTrue(foundFirstSplit, "Should have one first split");
    }

    @Test
    public void testGenerateFileSplits_XmlFormat() throws IOException {
        // Test with XML format that supports splitting
        String filePath = "/test/data.xml";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.XML;
        int splitSizeMB = 1; // 1MB splits

        // Mock file status for a 3MB file
        long fileSize = 3 * 1024 * 1024; // 3MB
        Mockito.when(hadoopFileSystemProxy.getFileStatus(filePath)).thenReturn(fileStatus);
        Mockito.when(fileStatus.getLen()).thenReturn(fileSize);

        Mockito.when(hadoopFileSystemProxy.getInputStream(filePath)).thenReturn(fsDataInputStream);
        Mockito.doNothing().when(fsDataInputStream).seek(Mockito.anyLong());
        Mockito.when(fsDataInputStream.read(any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        invocation -> {
                            byte[] buffer = invocation.getArgument(0);
                            int offset = invocation.getArgument(1);
                            buffer[offset] = '\n';
                            return 1;
                        });

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should create multiple splits for large XML files
        Assertions.assertTrue(splits.size() > 1);

        // Verify that at least one split is marked as first
        boolean foundFirstSplit = splits.stream().anyMatch(FileSourceSplit::isFirstSplit);
        Assertions.assertTrue(foundFirstSplit, "Should have one first split");
    }

    @Test
    public void testGenerateFileSplits_ZeroSplitSize() throws IOException {
        // Test with zero split size (should return single split)
        String filePath = "/test/data.csv";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.CSV;
        int splitSizeMB = 0;

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should return a single split when split size is 0
        Assertions.assertEquals(1, splits.size());
        FileSourceSplit split = splits.get(0);
        Assertions.assertEquals(tableId, split.getTableId());
        Assertions.assertEquals(filePath, split.getFilePath());
        Assertions.assertTrue(split.isCompleteFile());
        Assertions.assertTrue(split.isFirstSplit());
    }

    @Test
    public void testGenerateFileSplits_LargeFile() throws IOException {
        // Test with a large file that needs splitting
        String filePath = "/test/large.csv";
        String tableId = "test_table";
        FileFormat fileFormat = FileFormat.CSV;
        int splitSizeMB = 1; // 1MB splits

        // Mock file status for a 3MB file
        long fileSize = 3 * 1024 * 1024; // 3MB
        Mockito.when(hadoopFileSystemProxy.getFileStatus(filePath)).thenReturn(fileStatus);
        Mockito.when(fileStatus.getLen()).thenReturn(fileSize);

        // Create mock CSV content with line breaks at predictable positions
        String csvContent = createMockCsvContent(fileSize);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes());

        Mockito.when(hadoopFileSystemProxy.getInputStream(filePath)).thenReturn(fsDataInputStream);

        // Mock the FSDataInputStream behavior for seeking and reading
        Mockito.doNothing().when(fsDataInputStream).seek(Mockito.anyLong());
        Mockito.when(fsDataInputStream.read(any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        invocation -> {
                            byte[] buffer = invocation.getArgument(0);
                            int offset = invocation.getArgument(1);
                            int length = invocation.getArgument(2);
                            // Simulate finding line breaks every 100 bytes
                            buffer[offset] = '\n';
                            return 1;
                        });

        List<FileSourceSplit> splits =
                FileSplitUtils.generateFileSplits(
                        filePath, tableId, fileFormat, splitSizeMB, hadoopFileSystemProxy);

        // Should create multiple splits for large files
        Assertions.assertTrue(splits.size() > 1);

        // Verify split properties
        boolean foundFirstSplit = false;
        for (int i = 0; i < splits.size(); i++) {
            FileSourceSplit split = splits.get(i);
            Assertions.assertEquals(tableId, split.getTableId());
            Assertions.assertEquals(filePath, split.getFilePath());
            Assertions.assertFalse(split.isCompleteFile());

            if (split.isFirstSplit()) {
                Assertions.assertFalse(foundFirstSplit, "Should have only one first split");
                foundFirstSplit = true;
                Assertions.assertEquals(0L, split.getStartOffset());
            }
        }

        Assertions.assertTrue(foundFirstSplit, "Should have one first split");
    }

    private String createMockCsvContent(long targetSize) {
        StringBuilder sb = new StringBuilder();
        String line = "col1,col2,col3,col4,col5\n";

        while (sb.length() < targetSize) {
            sb.append(line);
        }

        return sb.toString();
    }

    private String createMockTextContent(long targetSize) {
        StringBuilder sb = new StringBuilder();
        String line = "This is a sample text line with some content\n";

        while (sb.length() < targetSize) {
            sb.append(line);
        }

        return sb.toString();
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;

/** Verifies the {@link FileSystem}-bound Parquet {@code OutputFile}/{@code InputFile} adapters. */
class FileSystemParquetFileTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void testWriteThenReadThroughTheAdapters() throws Exception {
        byte[] payload = "seatunnel".getBytes(StandardCharsets.UTF_8);
        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            FileSystem fs = proxy.getFileSystem();
            Path path = new Path(tempDir.resolve("adapter.bin").toString());

            try (PositionOutputStream out = new FileSystemOutputFile(fs, path).create(0L)) {
                out.write(payload);
            }

            FileSystemInputFile inputFile = FileSystemInputFile.fromPath(fs, path);
            Assertions.assertEquals(payload.length, inputFile.getLength());
            byte[] read = new byte[payload.length];
            try (SeekableInputStream in = inputFile.newStream()) {
                in.readFully(read);
            }
            Assertions.assertArrayEquals(payload, read);
        }
    }

    @Test
    void testCreateOrOverwriteReplacesExistingFile() throws Exception {
        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            FileSystem fs = proxy.getFileSystem();
            Path path = new Path(tempDir.resolve("overwrite.bin").toString());
            FileSystemOutputFile outputFile = new FileSystemOutputFile(fs, path);

            try (PositionOutputStream out = outputFile.create(0L)) {
                out.write("first-write".getBytes(StandardCharsets.UTF_8));
            }
            try (PositionOutputStream out = outputFile.createOrOverwrite(0L)) {
                out.write("second".getBytes(StandardCharsets.UTF_8));
            }

            Assertions.assertEquals(
                    "second".length(), FileSystemInputFile.fromPath(fs, path).getLength());
        }
    }

    @Test
    void testBlockSizeMatchesTheUnderlyingFileSystem() throws Exception {
        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            FileSystem fs = proxy.getFileSystem();
            Path path = new Path(tempDir.resolve("blocksize.bin").toString());
            FileSystemOutputFile outputFile = new FileSystemOutputFile(fs, path);

            // Mirrors HadoopOutputFile: only hdfs/webhdfs/viewfs report block-size support.
            Assertions.assertFalse(outputFile.supportsBlockSize());
            Assertions.assertEquals(fs.getDefaultBlockSize(path), outputFile.defaultBlockSize());
            Assertions.assertEquals(fs.makeQualified(path).toString(), outputFile.getPath());
        }
    }
}

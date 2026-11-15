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
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;

/** Verifies the {@link FileSystem}-bound Parquet {@code OutputFile}/{@code InputFile} adapters. */
class FileSystemParquetFileTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Hadoop local filesystem writes require winutils/HADOOP_HOME on Windows")
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
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Hadoop local filesystem writes require winutils/HADOOP_HOME on Windows")
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
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "Hadoop local filesystem writes require winutils/HADOOP_HOME on Windows")
    void testProxyOwnsASingleFileSystemSharedByManyFiles() throws Exception {
        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"))) {
            FileSystem fs = proxy.getFileSystem();
            // The proxy is the single owner: repeated lookups must not allocate a new
            // FileSystem. Per-file resolution (HadoopOutputFile/HadoopInputFile.fromPath, or
            // ParquetReader.builder(ReadSupport, Path)) allocated one per file and leaked its
            // metrics registration, which is what these adapters exist to avoid.
            Assertions.assertSame(fs, proxy.getFileSystem());

            // Many files, one FileSystem instance.
            for (int i = 0; i < 3; i++) {
                Path path = new Path(tempDir.resolve("shared-" + i + ".bin").toString());
                try (PositionOutputStream out = new FileSystemOutputFile(fs, path).create(0L)) {
                    out.write(("row" + i).getBytes(StandardCharsets.UTF_8));
                }
                Assertions.assertEquals(4, FileSystemInputFile.fromPath(fs, path).getLength());
                Assertions.assertSame(fs, proxy.getFileSystem());
            }
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

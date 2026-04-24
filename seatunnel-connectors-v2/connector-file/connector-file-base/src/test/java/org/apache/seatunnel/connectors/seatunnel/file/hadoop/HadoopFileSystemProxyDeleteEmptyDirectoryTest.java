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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

class HadoopFileSystemProxyDeleteEmptyDirectoryTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void shouldDeleteEmptyDirectory() throws Exception {
        java.nio.file.Path emptyDir = Files.createDirectory(tempDir.resolve("empty"));

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new LocalConf("file:///"))) {
            Assertions.assertTrue(proxy.deleteEmptyDirectory(emptyDir.toUri().toString()));
        }

        Assertions.assertFalse(Files.exists(emptyDir));
    }

    @Test
    void shouldKeepNonEmptyDirectory() throws Exception {
        java.nio.file.Path nonEmptyDir = Files.createDirectory(tempDir.resolve("non-empty"));
        java.nio.file.Path childFile = Files.createFile(nonEmptyDir.resolve("data.txt"));

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new LocalConf("file:///"))) {
            Assertions.assertFalse(proxy.deleteEmptyDirectory(nonEmptyDir.toUri().toString()));
        }

        Assertions.assertTrue(Files.exists(nonEmptyDir));
        Assertions.assertTrue(Files.exists(childFile));
    }

    @Test
    void shouldReturnFalseForMissingPathAndRegularFile() throws Exception {
        java.nio.file.Path regularFile = Files.createFile(tempDir.resolve("data.txt"));
        java.nio.file.Path missingPath = tempDir.resolve("missing");

        try (HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new LocalConf("file:///"))) {
            Assertions.assertFalse(proxy.deleteEmptyDirectory(missingPath.toUri().toString()));
            Assertions.assertFalse(proxy.deleteEmptyDirectory(regularFile.toUri().toString()));
        }

        Assertions.assertTrue(Files.exists(regularFile));
    }

    private static class LocalConf extends HadoopConf {
        private static final String LOCAL_FILE_SYSTEM_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String FILE_SCHEMA = "file";

        private LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return LOCAL_FILE_SYSTEM_IMPL;
        }

        @Override
        public String getSchema() {
            return FILE_SCHEMA;
        }
    }
}

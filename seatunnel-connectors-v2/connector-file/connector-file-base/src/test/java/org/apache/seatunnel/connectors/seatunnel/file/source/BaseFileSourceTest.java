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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

class BaseFileSourceTest {

    private static final String EMPTY_FILE_SYSTEM_URI = "empty:///";
    private static final String EMPTY_PATH = "empty:///empty-dir";
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

    @Test
    void testMarkdownSourceDiscoversSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createMarkdownConfig(false));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                MARKDOWN_FIELD_NAMES, catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void testMarkdownSourceDiscoversRagMetadataSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createMarkdownConfig(true));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                concat(MARKDOWN_FIELD_NAMES, MARKDOWN_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    private ReadonlyConfig createMarkdownConfig(boolean ragMetadataEnabled) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), EMPTY_PATH);
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        map.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), ragMetadataEnabled);
        return ReadonlyConfig.fromMap(map);
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static class TestFileSource extends BaseFileSource {

        private TestFileSource(ReadonlyConfig pluginConfig) {
            super(pluginConfig);
        }

        @Override
        protected HadoopConf initHadoopConf() {
            return new EmptyConf(EMPTY_FILE_SYSTEM_URI);
        }

        @Override
        public String getPluginName() {
            return "TestFileSource";
        }
    }

    private static class EmptyConf extends HadoopConf {

        private static final String SCHEMA = "empty";

        private EmptyConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return EmptyFileSystem.class.getName();
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }

    public static class EmptyFileSystem extends FileSystem {

        private URI uri;
        private Path workingDirectory;

        @Override
        public void initialize(URI name, Configuration conf) throws IOException {
            super.initialize(name, conf);
            this.uri = name;
            this.workingDirectory = new Path("/");
        }

        @Override
        public URI getUri() {
            return uri;
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize) {
            throw new UnsupportedOperationException("open is not needed for this test");
        }

        @Override
        public FSDataOutputStream create(
                Path path,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress) {
            throw new UnsupportedOperationException("create is not needed for this test");
        }

        @Override
        public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
            throw new UnsupportedOperationException("append is not needed for this test");
        }

        @Override
        public boolean rename(Path source, Path target) {
            throw new UnsupportedOperationException("rename is not needed for this test");
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            throw new UnsupportedOperationException("delete is not needed for this test");
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            return new FileStatus[0];
        }

        @Override
        public void setWorkingDirectory(Path newDirectory) {
            this.workingDirectory = newDirectory;
        }

        @Override
        public Path getWorkingDirectory() {
            return workingDirectory;
        }

        @Override
        public boolean mkdirs(Path path, FsPermission permission) {
            return true;
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            return new FileStatus(0, true, 1, 0, 0, path.makeQualified(uri, workingDirectory));
        }
    }
}

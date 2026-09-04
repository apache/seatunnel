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
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class BaseFileSourceTest {

    @TempDir private java.nio.file.Path tempDir;

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
    private static final String[] PDF_FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final String[] PDF_RAG_METADATA_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };

    @Test
    void testMarkdownSourceDiscoversSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createMarkdownConfig(false));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                MARKDOWN_FIELD_NAMES, catalogTable.getSeaTunnelRowType().getFieldNames());
        Assertions.assertTrue(catalogTable.getMetadataSchema().getColumns().isEmpty());
    }

    @Test
    void testMarkdownSourceDiscoversRagMetadataSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createMarkdownConfig(true));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                concat(MARKDOWN_FIELD_NAMES, MARKDOWN_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
        assertMarkdownKnowledgeSyncMetadata(catalogTable);
    }

    @Test
    void testMarkdownSourceDiscoversKnowledgeSyncMetadataFromExistingFile() throws Exception {
        java.nio.file.Path markdownFile = tempDir.resolve("document.md");
        Files.write(markdownFile, java.util.Arrays.asList("# Title"), StandardCharsets.UTF_8);
        BaseFileSource source =
                new LocalTestFileSource(createMarkdownConfig(markdownFile.toString(), true));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                concat(MARKDOWN_FIELD_NAMES, MARKDOWN_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
        assertMarkdownKnowledgeSyncMetadata(catalogTable);
    }

    @Test
    void testMarkdownSourceRejectsUnsafeUriWithoutExposingIt() {
        String unsafeUri = "https://user:secret@example.com/%zz?X-Amz-Signature=value#part";

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> new TestFileSource(createMarkdownConfig(unsafeUri, true)));

        Assertions.assertFalse(exception.getMessage().contains("user"));
        Assertions.assertFalse(exception.getMessage().contains("secret"));
        Assertions.assertFalse(exception.getMessage().contains("X-Amz-Signature"));
        Assertions.assertFalse(exception.getMessage().contains("value"));
        Assertions.assertFalse(exception.getMessage().contains("part"));
    }

    @Test
    void testMarkdownSourceRetainsSanitizedDiscoveryCause() {
        String source = "failing:///docs/a.md?token=secret-value#part";

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> new FailingTestFileSource(createMarkdownConfig(source, true)));

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains(IOException.class.getName()));
        Assertions.assertTrue(exception.getCause().getStackTrace().length > 0);
        Assertions.assertNotNull(exception.getCause().getCause());
        Assertions.assertTrue(exception.getCause().getCause().getStackTrace().length > 0);
        assertDoesNotExposeSensitiveSource(exception);
    }

    @Test
    void testPdfSourceDiscoversSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createPdfConfig(false));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                PDF_FIELD_NAMES, catalogTable.getSeaTunnelRowType().getFieldNames());
        Assertions.assertTrue(catalogTable.getMetadataSchema().getColumns().isEmpty());
    }

    @Test
    void testPdfSourceDiscoversRagMetadataSchemaFromEmptyDirectory() {
        BaseFileSource source = new TestFileSource(createPdfConfig(true));

        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);

        Assertions.assertArrayEquals(
                concat(PDF_FIELD_NAMES, PDF_RAG_METADATA_FIELD_NAMES),
                catalogTable.getSeaTunnelRowType().getFieldNames());
        Assertions.assertTrue(catalogTable.getMetadataSchema().getColumns().isEmpty());
    }

    private ReadonlyConfig createMarkdownConfig(boolean ragMetadataEnabled) {
        return createMarkdownConfig(EMPTY_PATH, ragMetadataEnabled);
    }

    private ReadonlyConfig createMarkdownConfig(String path, boolean ragMetadataEnabled) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), path);
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        map.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), ragMetadataEnabled);
        return ReadonlyConfig.fromMap(map);
    }

    private ReadonlyConfig createPdfConfig(boolean ragMetadataEnabled) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.FILE_PATH.key(), EMPTY_PATH);
        map.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "pdf");
        map.put(FileBaseSourceOptions.PDF_RAG_METADATA_ENABLED.key(), ragMetadataEnabled);
        return ReadonlyConfig.fromMap(map);
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static void assertMarkdownKnowledgeSyncMetadata(CatalogTable catalogTable) {
        List<String> metadataNames =
                catalogTable.getMetadataSchema().getColumns().stream()
                        .map(org.apache.seatunnel.api.table.catalog.Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertEquals(
                java.util.Arrays.asList(
                        KnowledgeSyncMetadataField.SOURCE_URI.getName(),
                        KnowledgeSyncMetadataField.DOCUMENT_ID.getName(),
                        KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(),
                        KnowledgeSyncMetadataField.CHUNK_HASH.getName()),
                metadataNames);
    }

    private static void assertDoesNotExposeSensitiveSource(Throwable throwable) {
        StringBuilder rendered = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            rendered.append(current.getMessage()).append('\n');
        }
        String message = rendered.toString();
        Assertions.assertFalse(message.contains("secret-value"));
        Assertions.assertFalse(message.contains("token="));
        Assertions.assertFalse(message.contains("#part"));
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

    private static class LocalTestFileSource extends BaseFileSource {

        private LocalTestFileSource(ReadonlyConfig pluginConfig) {
            super(pluginConfig);
        }

        @Override
        protected HadoopConf initHadoopConf() {
            return new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        }

        @Override
        public String getPluginName() {
            return "LocalTestFileSource";
        }
    }

    private static class FailingTestFileSource extends BaseFileSource {

        private FailingTestFileSource(ReadonlyConfig pluginConfig) {
            super(pluginConfig);
        }

        @Override
        protected HadoopConf initHadoopConf() {
            return new FailingConf("failing:///");
        }

        @Override
        public String getPluginName() {
            return "FailingTestFileSource";
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

    private static class LocalConf extends HadoopConf {

        private LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return "org.apache.hadoop.fs.LocalFileSystem";
        }

        @Override
        public String getSchema() {
            return "file";
        }
    }

    private static class FailingConf extends HadoopConf {

        private FailingConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return FailingFileSystem.class.getName();
        }

        @Override
        public String getSchema() {
            return "failing";
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
        public FileStatus getFileStatus(Path path) throws IOException {
            return new FileStatus(0, true, 1, 0, 0, path.makeQualified(uri, workingDirectory));
        }
    }

    public static class FailingFileSystem extends EmptyFileSystem {

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            throw new IOException("Access denied for " + path + " token=secret-value#part");
        }
    }
}

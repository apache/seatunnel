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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSourceTest.EmptyFileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSourceTest.FailingFileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseMultipleTableFileSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class BaseFileSourceConfigMetadataTest {

    @Test
    void shouldAddMetadataForBoundedEmptyMarkdownPath() {
        BaseFileSourceConfig empty = createConfig("empty:///bounded", "once", true, "empty");

        assertBridgeMetadata(empty.getCatalogTable());
    }

    @Test
    void shouldRetainMetadataForContinuousMarkdownFallback() {
        BaseFileSourceConfig continuous =
                createConfig("empty:///continuous", "continuous", true, "continuous");

        Assertions.assertTrue(continuous.getFilePaths().isEmpty());
        assertBridgeMetadata(continuous.getCatalogTable());
    }

    @Test
    void shouldAddMetadataToEveryEnabledMarkdownTableOnly() {
        BaseFileSourceConfig first = createConfig("empty:///first", "once", true, "first");
        BaseFileSourceConfig second = createConfig("empty:///second", "once", true, "second");
        BaseFileSourceConfig disabled =
                createConfig("empty:///disabled", "once", false, "disabled");
        BaseMultipleTableFileSourceConfig multipleConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleConfig.getFileSourceConfigs())
                .thenReturn(Arrays.asList(first, second, disabled));
        BaseMultipleTableFileSource source = new TestMultipleTableSource(multipleConfig);

        List<CatalogTable> catalogs = source.getProducedCatalogTables();

        assertBridgeMetadata(catalogs.get(0));
        assertBridgeMetadata(catalogs.get(1));
        Assertions.assertTrue(catalogs.get(2).getMetadataSchema().getColumns().isEmpty());
    }

    @Test
    void shouldRejectUnsafeUriBeforeDiscoveryWithoutExposingIt() {
        String unsafeUri = "https://user:secret@example.com/%zz?X-Amz-Signature=value#part";

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> createConfig(unsafeUri, "once", true, "unsafe"));

        Assertions.assertFalse(exception.getMessage().contains("user"));
        Assertions.assertFalse(exception.getMessage().contains("secret"));
        Assertions.assertFalse(exception.getMessage().contains("X-Amz-Signature"));
        Assertions.assertFalse(exception.getMessage().contains("value"));
        Assertions.assertFalse(exception.getMessage().contains("part"));
    }

    @Test
    void shouldRetainSanitizedDiscoveryCause() {
        String source = "failing:///docs/a.md?token=secret-value#part";

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class, () -> createFailingConfig(source, "failing"));

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains(IOException.class.getName()));
        Assertions.assertTrue(exception.getCause().getStackTrace().length > 0);
        Assertions.assertNotNull(exception.getCause().getCause());
        Assertions.assertTrue(exception.getCause().getCause().getStackTrace().length > 0);
        assertDoesNotExposeSensitiveSource(exception);
    }

    private BaseFileSourceConfig createConfig(
            String path, String discoveryMode, boolean enabled, String tableName) {
        Map<String, Object> values = new HashMap<>();
        values.put(FileBaseSourceOptions.FILE_PATH.key(), path);
        values.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        values.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), discoveryMode);
        values.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), enabled);
        return new TestFileSourceConfig(
                ReadonlyConfig.fromMap(values),
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                new String[] {"placeholder"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE
                                })));
    }

    private BaseFileSourceConfig createFailingConfig(String path, String tableName) {
        Map<String, Object> values = new HashMap<>();
        values.put(FileBaseSourceOptions.FILE_PATH.key(), path);
        values.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "markdown");
        values.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), true);
        return new FailingFileSourceConfig(
                ReadonlyConfig.fromMap(values),
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                new String[] {"placeholder"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE
                                })));
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

    private static void assertBridgeMetadata(CatalogTable catalogTable) {
        List<String> names =
                catalogTable.getMetadataSchema().getColumns().stream()
                        .map(org.apache.seatunnel.api.table.catalog.Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertEquals(
                Arrays.asList(
                        KnowledgeSyncMetadataField.SOURCE_URI.getName(),
                        KnowledgeSyncMetadataField.DOCUMENT_ID.getName(),
                        KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(),
                        KnowledgeSyncMetadataField.CHUNK_HASH.getName()),
                names);
    }

    private static class TestFileSourceConfig extends BaseFileSourceConfig {

        private TestFileSourceConfig(
                ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig) {
            super(readonlyConfig, catalogTableFromConfig);
        }

        @Override
        public HadoopConf getHadoopConfig() {
            return new EmptyConf("empty:///");
        }

        @Override
        public String getPluginName() {
            return "test-local-file";
        }
    }

    private static class FailingFileSourceConfig extends BaseFileSourceConfig {

        private FailingFileSourceConfig(
                ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig) {
            super(readonlyConfig, catalogTableFromConfig);
        }

        @Override
        public HadoopConf getHadoopConfig() {
            return new FailingConf("failing:///");
        }

        @Override
        public String getPluginName() {
            return "test-failing-file";
        }
    }

    private static class TestMultipleTableSource extends BaseMultipleTableFileSource {

        private TestMultipleTableSource(BaseMultipleTableFileSourceConfig config) {
            super(config);
        }

        @Override
        public String getPluginName() {
            return "test-local-file";
        }
    }

    private static class EmptyConf extends HadoopConf {

        private EmptyConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return EmptyFileSystem.class.getName();
        }

        @Override
        public String getSchema() {
            return "empty";
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
}

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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.FileSourceDocumentRouting;

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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

class MultipleTableFileSourceSplitEnumeratorTest {

    private static final String COUNTING_FILE_SYSTEM_URI = "counting:///";
    private static final String SOURCE_PATH = "counting:///source";

    @Test
    void assignSplitTest() throws Exception {
        int parallelism = 4;
        int fileSize = 50;

        List<String> filePaths = new ArrayList<>();
        IntStream.range(0, fileSize).forEach(i -> filePaths.add("filePath" + i));

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);

        Mockito.when(baseFileSourceConfig.getFilePathsForSplitEnumerator()).thenReturn(filePaths);

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "test", "hive_table1"),
                        null,
                        Maps.newHashMap(),
                        Lists.newArrayList(),
                        null);
        Mockito.when(baseFileSourceConfig.getCatalogTable()).thenReturn(catalogTable);

        BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);

        Mockito.when(baseMultipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Arrays.asList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        Mockito.when(context.currentParallelism()).thenReturn(parallelism);
        MultipleTableFileSourceSplitEnumerator enumerator =
                new MultipleTableFileSourceSplitEnumerator(
                        context, baseMultipleTableFileSourceConfig, new DefaultFileSplitStrategy());

        enumerator.open();
        Assertions.assertEquals(50, enumerator.currentUnassignedSplitSize());
        IntStream.range(0, parallelism).forEach(enumerator::registerReader);
        enumerator.run();

        ArgumentCaptor<Integer> subtaskId = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<List> split = ArgumentCaptor.forClass(List.class);

        Mockito.verify(context, Mockito.times(parallelism))
                .assignSplit(subtaskId.capture(), split.capture());

        List<Integer> subTaskAllValues = subtaskId.getAllValues();
        List<List> splitAllValues = split.getAllValues();

        for (int i = 0; i < parallelism; i++) {
            Assertions.assertEquals(i, subTaskAllValues.get(i));
            Assertions.assertEquals(
                    allocateFiles(i, parallelism, fileSize), splitAllValues.get(i).size());
        }

        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void assignSplitByDocumentRouteForEnabledTable() throws Exception {
        int parallelism = 4;
        List<String> filePaths =
                Arrays.asList(
                        "file:/tmp/knowledge/table/doc-a.md",
                        "file:/tmp/knowledge/table/doc-b.md",
                        "file:/tmp/knowledge/table/doc-c.md");

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getFilePathsForSplitEnumerator()).thenReturn(filePaths);

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "test", "hive_table1"),
                        null,
                        Maps.newHashMap(),
                        Lists.newArrayList(),
                        null);
        String tableId = catalogTable.getTableId().toTablePath().toString();
        Mockito.when(baseFileSourceConfig.getCatalogTable()).thenReturn(catalogTable);

        BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(baseMultipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(parallelism);
        MultipleTableFileSourceSplitEnumerator enumerator =
                new MultipleTableFileSourceSplitEnumerator(
                        context,
                        baseMultipleTableFileSourceConfig,
                        new DefaultFileSplitStrategy(),
                        Collections.singleton(tableId));

        enumerator.open();
        enumerator.run();

        ArgumentCaptor<Integer> subtaskId = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<List> split = ArgumentCaptor.forClass(List.class);
        Mockito.verify(context, Mockito.times(parallelism))
                .assignSplit(subtaskId.capture(), split.capture());

        Map<Integer, List<FileSourceSplit>> assignedSplits = new HashMap<>();
        for (int i = 0; i < subtaskId.getAllValues().size(); i++) {
            assignedSplits.put(subtaskId.getAllValues().get(i), split.getAllValues().get(i));
        }

        for (String filePath : filePaths) {
            String documentId = FileSourceDocumentRouting.buildDocumentId(filePath);
            int expectedOwner = FileSourceDocumentRouting.routeBucket(documentId, parallelism);
            Assertions.assertTrue(
                    assignedSplits.get(expectedOwner).stream()
                            .anyMatch(
                                    fileSourceSplit ->
                                            fileSourceSplit.getFilePath().equals(filePath)
                                                    && fileSourceSplit
                                                            .getTableId()
                                                            .equals(tableId)),
                    "File should be assigned to the reader that owns its document route bucket.");
        }
    }

    @Test
    void deferredFileDiscoveryDoesNotListFilesDuringConfigCreation() throws IOException {
        CountingFileSystem.reset();

        BaseFileSourceConfig sourceConfig =
                new DeferredFileSourceConfig(
                        createBinaryConfig(), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertTrue(sourceConfig.isFileDiscoveryDeferred());
        Assertions.assertTrue(sourceConfig.getFilePaths().isEmpty());
        Assertions.assertEquals(0, CountingFileSystem.listStatusCount);
        Assertions.assertEquals(0, CountingFileSystem.closeCount);

        List<String> filePaths = sourceConfig.getFilePathsForSplitEnumerator();

        Assertions.assertEquals(2, filePaths.size());
        Assertions.assertEquals(1, CountingFileSystem.listStatusCount);
        Assertions.assertEquals(1, CountingFileSystem.closeCount);

        Assertions.assertEquals(
                2, sourceConfig.getReadStrategy().getFileNamesByPath(SOURCE_PATH).size());
        Assertions.assertEquals(2, CountingFileSystem.listStatusCount);
        Assertions.assertEquals(1, CountingFileSystem.closeCount);
    }

    @Test
    void deferredFileDiscoveryClosesResourcesWhenDiscoveryFails() {
        CountingFileSystem.reset();
        CountingFileSystem.failOnListStatus = true;
        BaseFileSourceConfig sourceConfig =
                new DeferredFileSourceConfig(
                        createBinaryConfig(), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertThrows(
                FileConnectorException.class, sourceConfig::getFilePathsForSplitEnumerator);
        Assertions.assertEquals(1, CountingFileSystem.closeCount);
    }

    @Test
    void eagerFileDiscoveryKeepsExistingConfigCreationBehavior() {
        CountingFileSystem.reset();

        BaseFileSourceConfig sourceConfig =
                new EagerFileSourceConfig(
                        createBinaryConfig(), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertFalse(sourceConfig.isFileDiscoveryDeferred());
        Assertions.assertEquals(2, sourceConfig.getFilePaths().size());
        Assertions.assertEquals(1, CountingFileSystem.listStatusCount);

        sourceConfig.getFilePathsForSplitEnumerator();

        Assertions.assertEquals(1, CountingFileSystem.listStatusCount);
    }

    @Test
    void splitEnumeratorDiscoversDeferredFilePathsOnOpen() {
        CountingFileSystem.reset();
        BaseMultipleTableFileSourceConfig multipleTableConfig =
                createMultipleTableConfig(DeferredFileSourceConfig::new);

        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        MultipleTableFileSourceSplitEnumerator enumerator =
                new MultipleTableFileSourceSplitEnumerator(
                        context, multipleTableConfig, new DefaultFileSplitStrategy());

        Assertions.assertEquals(0, CountingFileSystem.listStatusCount);
        Assertions.assertEquals(0, CountingFileSystem.closeCount);

        enumerator.open();

        Assertions.assertEquals(1, CountingFileSystem.listStatusCount);
        Assertions.assertEquals(1, CountingFileSystem.closeCount);
        Assertions.assertEquals(2, enumerator.currentUnassignedSplitSize());
    }

    private int allocateFiles(int id, int parallelism, int fileSize) {
        int filesPerIteration = fileSize / parallelism;
        int remainder = fileSize % parallelism;

        if (id < remainder) {
            return filesPerIteration + 1;
        }
        return filesPerIteration;
    }

    private static ReadonlyConfig createBinaryConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), SOURCE_PATH);
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "binary");
        return ReadonlyConfig.fromMap(config);
    }

    private static BaseMultipleTableFileSourceConfig createMultipleTableConfig(
            FileSourceConfigFactory fileSourceConfigFactory) {
        return new BaseMultipleTableFileSourceConfig(
                createBinaryConfig(),
                Collections.singletonList(CatalogTableUtil.buildSimpleTextTable())) {

            @Override
            public BaseFileSourceConfig getBaseSourceConfig(
                    ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig) {
                return fileSourceConfigFactory.create(readonlyConfig, catalogTableFromConfig);
            }
        };
    }

    private interface FileSourceConfigFactory {

        BaseFileSourceConfig create(
                ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig);
    }

    private static class DeferredFileSourceConfig extends EagerFileSourceConfig {

        private DeferredFileSourceConfig(
                ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig) {
            super(readonlyConfig, catalogTableFromConfig);
        }

        @Override
        protected boolean shouldDeferFileDiscovery(ReadonlyConfig readonlyConfig) {
            return true;
        }
    }

    private static class EagerFileSourceConfig extends BaseFileSourceConfig {

        private static final long serialVersionUID = 1L;

        private EagerFileSourceConfig(
                ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig) {
            super(readonlyConfig, catalogTableFromConfig);
        }

        @Override
        public HadoopConf getHadoopConfig() {
            return new CountingConf();
        }

        @Override
        public String getPluginName() {
            return "CountingFile";
        }
    }

    private static class CountingConf extends HadoopConf {

        private static final String SCHEMA = "counting";

        private CountingConf() {
            super(COUNTING_FILE_SYSTEM_URI);
        }

        @Override
        public String getFsHdfsImpl() {
            return CountingFileSystem.class.getName();
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }

    public static class CountingFileSystem extends FileSystem {

        private static int listStatusCount;
        private static int closeCount;
        private static boolean failOnListStatus;

        private URI uri;
        private Path workingDirectory;
        private boolean closed;

        private static void reset() {
            listStatusCount = 0;
            closeCount = 0;
            failOnListStatus = false;
        }

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
        public FileStatus[] listStatus(Path path) throws IOException {
            if (closed) {
                throw new IOException("File system is closed");
            }
            listStatusCount++;
            if (failOnListStatus) {
                throw new IOException("File discovery failed");
            }
            return new FileStatus[] {fileStatus(path, "first.bin"), fileStatus(path, "second.bin")};
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

        @Override
        public void close() throws IOException {
            closed = true;
            closeCount++;
            super.close();
        }

        private FileStatus fileStatus(Path parent, String fileName) {
            Path path = new Path(parent, fileName).makeQualified(uri, workingDirectory);
            return new FileStatus(1, false, 1, 0, 0, path);
        }
    }
}

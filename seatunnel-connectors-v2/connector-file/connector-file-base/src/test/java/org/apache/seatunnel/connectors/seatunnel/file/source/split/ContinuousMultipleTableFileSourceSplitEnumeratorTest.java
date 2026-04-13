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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@DisabledOnOs(OS.WINDOWS)
class ContinuousMultipleTableFileSourceSplitEnumeratorTest {

    @TempDir private Path tempDir;

    @Test
    void testScanOnceEnqueueAssignAndAck() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst"));
        Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir);
        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                enumeratorWithContext.enumerator;
        try {
            enumerator.scanOnceForTest();
            Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());

            enumerator.handleSplitRequest(0);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<java.util.List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) java.util.List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            Assertions.assertEquals(1, splitsCaptor.getValue().size());

            FileSourceSplit assigned = splitsCaptor.getValue().get(0);
            FileSourceState state = enumerator.snapshotState(1L);
            Assertions.assertTrue(
                    state.getAssignedSplit().stream()
                            .anyMatch(s -> s.splitId().equals(assigned.splitId())));

            enumerator.handleSourceEvent(0, new FileSplitFinishedEvent(assigned.splitId()));
            FileSourceState stateAfterAck = enumerator.snapshotState(2L);
            Assertions.assertTrue(stateAfterAck.getAssignedSplit().isEmpty());
        } finally {
            enumerator.close();
        }
    }

    @Test
    void testScanOnceSkipsWhenTargetIsNewerInDistcp() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src2"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst2"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");

        Files.write(srcFile, "abc".getBytes());
        Files.write(dstFile, "abc".getBytes());

        long now = System.currentTimeMillis();
        Files.setLastModifiedTime(srcFile, FileTime.fromMillis(now - 10_000));
        Files.setLastModifiedTime(dstFile, FileTime.fromMillis(now));

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir);
        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                enumeratorWithContext.enumerator;
        try {
            enumerator.scanOnceForTest();
            Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        } finally {
            enumerator.close();
        }
    }

    @Test
    void testScanOnceDoesNotRequeueSameVersionAfterAck() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src2_requeue"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst2_requeue"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir);
        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                enumeratorWithContext.enumerator;
        try {
            enumerator.scanOnceForTest();
            Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());

            enumerator.handleSplitRequest(0);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<java.util.List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) java.util.List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);

            enumerator.handleSourceEvent(0, new FileSplitFinishedEvent(assigned.splitId()));

            // Same file version should not be queued again before target catches up.
            enumerator.scanOnceForTest();
            Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());

            // Once source file version changes, it should be discovered again.
            Files.write(srcFile, "abcd".getBytes());
            Files.setLastModifiedTime(
                    srcFile, FileTime.fromMillis(System.currentTimeMillis() + 2000));
            enumerator.scanOnceForTest();
            Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
        } finally {
            enumerator.close();
        }
    }

    @Test
    void testScanOnceCleansKnownVersionWhenAckedSourceFileDisappears() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src2_cleanup"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst2_cleanup"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir);
        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                enumeratorWithContext.enumerator;
        try {
            enumerator.scanOnceForTest();
            enumerator.handleSplitRequest(0);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<java.util.List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) java.util.List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);

            enumerator.handleSourceEvent(0, new FileSplitFinishedEvent(assigned.splitId()));
            Assertions.assertEquals(1, getKnownSplitVersionSize(enumerator));

            Files.delete(srcFile);
            enumerator.scanOnceForTest();

            Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
            Assertions.assertEquals(0, getKnownSplitVersionSize(enumerator));
        } finally {
            enumerator.close();
        }
    }

    @Test
    void testContinuousDiscoveryRequiresBinaryFormat() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src3"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst3"));

        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), srcDir.toString());
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "json");
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");
        config.put(FileBaseSourceOptions.TARGET_PATH.key(), dstDir.toString());
        config.put(FileBaseSourceOptions.UPDATE_STRATEGY.key(), "distcp");
        config.put(FileBaseSourceOptions.COMPARE_MODE.key(), "len_mtime");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getBaseFileSourceConfig()).thenReturn(readonlyConfig);
        Mockito.when(baseFileSourceConfig.getHadoopConfig())
                .thenReturn(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", "table"),
                        null,
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        Mockito.when(baseFileSourceConfig.getCatalogTable()).thenReturn(catalogTable);

        BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(1);

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                new ContinuousMultipleTableFileSourceSplitEnumerator(
                                        context,
                                        multipleTableFileSourceConfig,
                                        new DefaultFileSplitStrategy()));
        Assertions.assertTrue(
                exception.getMessage().contains("file_format_type=binary"),
                "continuous mode should require binary format");
    }

    @Test
    void testRestoreKeepsLatestStartBaseline() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src4"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst4"));

        EnumeratorWithContext first = createEnumerator(srcDir, dstDir, "latest");
        try {
            Thread.sleep(1200L);
            Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

            FileSourceState checkpointState = first.enumerator.snapshotState(1L);
            Assertions.assertTrue(
                    checkpointState.getDiscoveryStartTimeMillis() > 0,
                    "checkpoint should persist discovery baseline");

            EnumeratorWithContext restored =
                    createEnumerator(srcDir, dstDir, "latest", checkpointState);
            try {
                restored.enumerator.scanOnceForTest();
                Assertions.assertEquals(
                        1,
                        restored.enumerator.currentUnassignedSplitSize(),
                        "restored enumerator should still discover files created after the original baseline");
            } finally {
                restored.enumerator.close();
            }
        } finally {
            first.enumerator.close();
        }
    }

    @Test
    void testRestoreReEnqueuesInFlightSplitsAsPending() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src5_restore_pending"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst5_restore_pending"));
        Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

        EnumeratorWithContext first = createEnumerator(srcDir, dstDir, "earliest");
        try {
            first.enumerator.scanOnceForTest();
            first.enumerator.handleSplitRequest(0);
            FileSourceState checkpointState = first.enumerator.snapshotState(1L);
            Assertions.assertFalse(
                    checkpointState.getAssignedSplit().isEmpty(),
                    "checkpoint should contain in-flight split before finished event arrives");

            EnumeratorWithContext restored =
                    createEnumerator(srcDir, dstDir, "earliest", checkpointState);
            try {
                Assertions.assertEquals(
                        1,
                        restored.enumerator.currentUnassignedSplitSize(),
                        "restored enumerator should re-enqueue in-flight split as pending");

                restored.enumerator.handleSplitRequest(0);
                @SuppressWarnings("unchecked")
                ArgumentCaptor<java.util.List<FileSourceSplit>> splitsCaptor =
                        ArgumentCaptor.forClass((Class) java.util.List.class);
                Mockito.verify(restored.context).assignSplit(Mockito.eq(0), splitsCaptor.capture());
                Assertions.assertEquals(1, splitsCaptor.getValue().size());
            } finally {
                restored.enumerator.close();
            }
        } finally {
            first.enumerator.close();
        }
    }

    @Test
    void testScanOnceAssignsSplitAfterEarlyRequest() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src6_assign_after_scan"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst6_assign_after_scan"));
        Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir, "earliest");
        try {
            Mockito.when(enumeratorWithContext.context.registeredReaders())
                    .thenReturn(Collections.singleton(0));

            // Simulate reader requests splits before the first discovery scan.
            enumeratorWithContext.enumerator.handleSplitRequest(0);

            // Discovery should enqueue and proactively assign to registered readers.
            enumeratorWithContext.enumerator.scanOnceForTest();

            @SuppressWarnings("unchecked")
            ArgumentCaptor<java.util.List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) java.util.List.class);
            Mockito.verify(enumeratorWithContext.context, Mockito.atLeastOnce())
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            Assertions.assertFalse(splitsCaptor.getValue().isEmpty());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testRestoreSkipsRecoveredSplitWhenAlreadySynced() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src7_restore_synced"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst7_restore_synced"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        EnumeratorWithContext first = createEnumerator(srcDir, dstDir, "earliest");
        try {
            first.enumerator.scanOnceForTest();
            first.enumerator.handleSplitRequest(0);
            FileSourceState checkpointState = first.enumerator.snapshotState(1L);
            Assertions.assertFalse(
                    checkpointState.getAssignedSplit().isEmpty(),
                    "checkpoint should contain in-flight split before finished event arrives");

            Path dstFile = dstDir.resolve("test.bin");
            Files.copy(srcFile, dstFile, StandardCopyOption.REPLACE_EXISTING);
            FileTime sourceMtime = Files.getLastModifiedTime(srcFile);
            Files.setLastModifiedTime(dstFile, FileTime.fromMillis(sourceMtime.toMillis() + 1000));

            EnumeratorWithContext restored =
                    createEnumerator(srcDir, dstDir, "earliest", checkpointState);
            try {
                Assertions.assertEquals(
                        0,
                        restored.enumerator.currentUnassignedSplitSize(),
                        "restored enumerator should not re-enqueue splits that are already synced");
            } finally {
                restored.enumerator.close();
            }
        } finally {
            first.enumerator.close();
        }
    }

    @Test
    void testContinuousDiscoveryRequiresPositiveScanInterval() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src8"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst8"));

        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), srcDir.toString());
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "binary");
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        config.put(FileBaseSourceOptions.START_MODE.key(), "earliest");
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");
        config.put(FileBaseSourceOptions.TARGET_PATH.key(), dstDir.toString());
        config.put(FileBaseSourceOptions.UPDATE_STRATEGY.key(), "distcp");
        config.put(FileBaseSourceOptions.COMPARE_MODE.key(), "len_mtime");
        config.put(FileBaseSourceOptions.SCAN_INTERVAL.key(), "0S");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getBaseFileSourceConfig()).thenReturn(readonlyConfig);
        Mockito.when(baseFileSourceConfig.getHadoopConfig())
                .thenReturn(new LocalConf(FS_DEFAULT_NAME_DEFAULT));

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", "table"),
                        null,
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        Mockito.when(baseFileSourceConfig.getCatalogTable()).thenReturn(catalogTable);

        BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                new ContinuousMultipleTableFileSourceSplitEnumerator(
                                        context,
                                        multipleTableFileSourceConfig,
                                        new DefaultFileSplitStrategy()));
        Assertions.assertTrue(
                exception.getMessage().contains("scan_interval > 0"),
                "continuous mode should require a positive scan_interval");
    }

    private EnumeratorWithContext createEnumerator(Path srcDir, Path dstDir) throws IOException {
        return createEnumerator(srcDir, dstDir, "earliest");
    }

    private EnumeratorWithContext createEnumerator(Path srcDir, Path dstDir, String startMode)
            throws IOException {
        return createEnumerator(
                srcDir, dstDir, startMode, new FileSourceState(Collections.emptySet()));
    }

    private EnumeratorWithContext createEnumerator(
            Path srcDir, Path dstDir, String startMode, FileSourceState checkpointState)
            throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), srcDir.toString());
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "binary");
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        config.put(FileBaseSourceOptions.START_MODE.key(), startMode);
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");
        config.put(FileBaseSourceOptions.TARGET_PATH.key(), dstDir.toString());
        config.put(FileBaseSourceOptions.UPDATE_STRATEGY.key(), "distcp");
        config.put(FileBaseSourceOptions.COMPARE_MODE.key(), "len_mtime");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getBaseFileSourceConfig()).thenReturn(readonlyConfig);
        Mockito.when(baseFileSourceConfig.getHadoopConfig())
                .thenReturn(new LocalConf(FS_DEFAULT_NAME_DEFAULT));

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", "table"),
                        null,
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        Mockito.when(baseFileSourceConfig.getCatalogTable()).thenReturn(catalogTable);

        BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(1);

        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                new ContinuousMultipleTableFileSourceSplitEnumerator(
                        context,
                        multipleTableFileSourceConfig,
                        new DefaultFileSplitStrategy(),
                        checkpointState);
        return new EnumeratorWithContext(enumerator, context);
    }

    private static final class EnumeratorWithContext {
        private final ContinuousMultipleTableFileSourceSplitEnumerator enumerator;
        private final SourceSplitEnumerator.Context<FileSourceSplit> context;

        private EnumeratorWithContext(
                ContinuousMultipleTableFileSourceSplitEnumerator enumerator,
                SourceSplitEnumerator.Context<FileSourceSplit> context) {
            this.enumerator = enumerator;
            this.context = context;
        }
    }

    @SuppressWarnings("unchecked")
    private static int getKnownSplitVersionSize(
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator)
            throws NoSuchFieldException, IllegalAccessException {
        Field field =
                ContinuousMultipleTableFileSourceSplitEnumerator.class.getDeclaredField(
                        "knownSplitVersions");
        field.setAccessible(true);
        return ((Map<String, Object>) field.get(enumerator)).size();
    }

    private static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}

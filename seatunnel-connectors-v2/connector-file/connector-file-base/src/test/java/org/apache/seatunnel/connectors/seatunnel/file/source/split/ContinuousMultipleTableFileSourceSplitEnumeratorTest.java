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
import org.apache.seatunnel.connectors.seatunnel.file.config.FilePostSyncAction;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceOperationState;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import org.apache.hadoop.fs.FileStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    void testLocalTextTailingEmitsOnlyCompleteAppendedRows() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail"));
        Path srcFile = srcDir.resolve("application.log");
        Files.write(srcFile, "first\npartial".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                    enumeratorWithContext.enumerator;
            enumerator.scanOnceForTest();
            FileSourceSplit firstSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals(0L, firstSplit.getStart());
            Assertions.assertEquals("first\n".getBytes().length, firstSplit.getLength());

            enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    0,
                    enumerator.currentUnassignedSplitSize(),
                    "a file must not receive another range while its previous range is in flight");

            enumerator.handleSourceEvent(0, new FileSplitFinishedEvent(firstSplit.splitId()));
            Files.write(srcFile, "-done\n".getBytes(), StandardOpenOption.APPEND);
            enumerator.scanOnceForTest();

            FileSourceSplit appendedSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals(firstSplit.getLength(), appendedSplit.getStart());
            Assertions.assertEquals("partial-done\n".getBytes().length, appendedSplit.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingRestoresCommittedOffset() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_restore"));
        Path srcFile = srcDir.resolve("application.log");
        Files.write(srcFile, "first\n".getBytes());

        FileSourceState checkpointState;
        EnumeratorWithContext first =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            first.enumerator.scanOnceForTest();
            FileSourceSplit split = assignAndCaptureSingleSplit(first);
            first.enumerator.handleSourceEvent(0, new FileSplitFinishedEvent(split.splitId()));
            checkpointState = first.enumerator.snapshotState(1L);
        } finally {
            first.enumerator.close();
        }

        Files.write(srcFile, "second\n".getBytes(), StandardOpenOption.APPEND);
        EnumeratorWithContext restored =
                createTextTailingEnumerator(srcDir, "earliest", checkpointState);
        try {
            restored.enumerator.scanOnceForTest();
            FileSourceSplit split = assignAndCaptureSingleSplit(restored);
            Assertions.assertEquals("first\n".getBytes().length, split.getStart());
            Assertions.assertEquals("second\n".getBytes().length, split.getLength());
        } finally {
            restored.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingLatestStartsAfterExistingContent() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_latest"));
        Path srcFile = srcDir.resolve("application.log");
        Files.write(srcFile, "existing\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "latest", new FileSourceState(Collections.emptySet()));
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    0, enumeratorWithContext.enumerator.currentUnassignedSplitSize());

            Files.write(srcFile, "new\n".getBytes(), StandardOpenOption.APPEND);
            enumeratorWithContext.enumerator.scanOnceForTest();
            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals("existing\n".getBytes().length, split.getStart());
            Assertions.assertEquals("new\n".getBytes().length, split.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingSkipsConfiguredHeader() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_header"));
        Files.write(srcDir.resolve("application.log"), "header\nfirst\n".getBytes());
        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key(), 1L);

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals("header\n".getBytes().length, split.getStart());
            Assertions.assertEquals("first\n".getBytes().length, split.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingFollowsFileIdentityAcrossRotation() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_rotation"));
        Path activeFile = srcDir.resolve("application.log");
        Path rotatedFile = srcDir.resolve("application.log.1");
        Files.write(activeFile, "first\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            FileSourceSplit firstSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(firstSplit.splitId()));

            Files.write(activeFile, "last-old\n".getBytes(), StandardOpenOption.APPEND);
            Files.move(activeFile, rotatedFile);
            Files.write(activeFile, "first-new\n".getBytes());

            enumeratorWithContext.enumerator.scanOnceForTest();
            List<FileSourceSplit> splits = assignAndCaptureSplits(enumeratorWithContext);
            Assertions.assertEquals(2, splits.size());

            FileSourceSplit rotatedSplit =
                    splits.stream()
                            .filter(split -> split.getFilePath().endsWith("application.log.1"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            Assertions.assertEquals("first\n".getBytes().length, rotatedSplit.getStart());
            Assertions.assertEquals("last-old\n".getBytes().length, rotatedSplit.getLength());

            FileSourceSplit newSplit =
                    splits.stream()
                            .filter(split -> split.getFilePath().endsWith("application.log"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            Assertions.assertEquals(0L, newSplit.getStart());
            Assertions.assertEquals("first-new\n".getBytes().length, newSplit.getLength());
            Assertions.assertNotEquals(rotatedSplit.getFileIdentity(), newSplit.getFileIdentity());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingDetectsCopyTruncateAfterRefill() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_copytruncate"));
        Path srcFile = srcDir.resolve("application.log");
        Files.write(srcFile, "old-record\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            FileSourceSplit firstSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(firstSplit.splitId()));

            Files.write(
                    srcFile,
                    "new-record-one\nnew-record-two\n".getBytes(),
                    StandardOpenOption.TRUNCATE_EXISTING);
            enumeratorWithContext.enumerator.scanOnceForTest();

            FileSourceSplit rewrittenSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals(0L, rewrittenSplit.getStart());
            Assertions.assertEquals(Files.size(srcFile), rewrittenSplit.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingLatestDiscardsExistingPartialRow() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_latest_partial"));
        Path srcFile = srcDir.resolve("application.log");
        String existingPartial = "existing-partial";
        Files.write(srcFile, existingPartial.getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "latest", new FileSourceState(Collections.emptySet()));
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    0, enumeratorWithContext.enumerator.currentUnassignedSplitSize());

            Files.write(srcFile, "-done\nnext\n".getBytes(), StandardOpenOption.APPEND);
            enumeratorWithContext.enumerator.scanOnceForTest();

            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals(
                    (existingPartial + "-done\n").getBytes().length, split.getStart());
            Assertions.assertEquals("next\n".getBytes().length, split.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingPrunesMissingFileState() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_prune"));
        Path srcFile = srcDir.resolve("application.log");
        Files.write(srcFile, new byte[0]);

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    1,
                    enumeratorWithContext.enumerator.snapshotState(1L).getFileTailStates().size());

            Files.delete(srcFile);
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.scanOnceForTest();

            Assertions.assertTrue(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(2L)
                            .getFileTailStates()
                            .isEmpty());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingContinuesAfterFileDisappearsDuringScan() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_partial_scan"));
        Path staleFile = srcDir.resolve("stale.log");
        Path activeFile = srcDir.resolve("active.log");
        Files.write(staleFile, new byte[0]);
        Files.write(activeFile, "first\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                    enumeratorWithContext.enumerator;
            enumerator.scanOnceForTest();
            FileSourceSplit firstSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            enumerator.handleSourceEvent(0, new FileSplitFinishedEvent(firstSplit.splitId()));

            HadoopFileSystemProxy sourceFs = getTableScanContextFileSystem(enumerator, "sourceFs");
            FileStatus staleStatus =
                    findFileStatus(sourceFs.listStatus(srcDir.toString()), "stale.log");

            Files.delete(staleFile);
            Files.write(activeFile, "second\n".getBytes(), StandardOpenOption.APPEND);
            FileStatus activeStatus =
                    findFileStatus(sourceFs.listStatus(srcDir.toString()), "active.log");

            HadoopFileSystemProxy sourceFsSpy = Mockito.spy(sourceFs);
            Mockito.doReturn(new FileStatus[] {staleStatus, activeStatus})
                    .when(sourceFsSpy)
                    .listStatus(srcDir.toString());
            setTableScanContextFileSystem(enumerator, "sourceFs", sourceFsSpy);

            enumerator.scanOnceForTest();

            FileSourceSplit appendedSplit = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals("first\n".getBytes().length, appendedSplit.getStart());
            Assertions.assertEquals("second\n".getBytes().length, appendedSplit.getLength());
            Assertions.assertEquals(
                    2,
                    enumerator.snapshotState(1L).getFileTailStates().size(),
                    "an incomplete scan must not prune state for a file that disappeared");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingContinuesAfterFileInspectionRuntimeFailure() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_runtime_failure"));
        Path failedFile = srcDir.resolve("failed.log");
        Path activeFile = srcDir.resolve("active.log");
        Files.write(failedFile, "failed\n".getBytes());
        Files.write(activeFile, "active\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                    enumeratorWithContext.enumerator;
            HadoopFileSystemProxy sourceFs = getTableScanContextFileSystem(enumerator, "sourceFs");
            FileStatus failedStatus =
                    findFileStatus(sourceFs.listStatus(srcDir.toString()), "failed.log");
            FileStatus activeStatus =
                    findFileStatus(sourceFs.listStatus(srcDir.toString()), "active.log");

            HadoopFileSystemProxy sourceFsSpy = Mockito.spy(sourceFs);
            Mockito.doReturn(new FileStatus[] {failedStatus, activeStatus})
                    .when(sourceFsSpy)
                    .listStatus(srcDir.toString());
            Mockito.doThrow(new IllegalStateException("failed to inspect file"))
                    .when(sourceFsSpy)
                    .getInputStream(failedStatus.getPath().toString());
            setTableScanContextFileSystem(enumerator, "sourceFs", sourceFsSpy);

            Assertions.assertDoesNotThrow(enumerator::scanOnceForTest);

            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals(activeStatus.getPath().toString(), split.getFilePath());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingLatestRetainsBaselineAfterPartialScanFailure() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_latest_retry"));
        Path failedFile = srcDir.resolve("failed.log");
        Path activeFile = srcDir.resolve("active.log");
        Files.write(failedFile, "existing-failed\n".getBytes());
        Files.write(activeFile, "existing-active\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "latest", new FileSourceState(Collections.emptySet()));
        try {
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                    enumeratorWithContext.enumerator;
            HadoopFileSystemProxy sourceFs = getTableScanContextFileSystem(enumerator, "sourceFs");
            FileStatus failedStatus =
                    findFileStatus(sourceFs.listStatus(srcDir.toString()), "failed.log");
            FileStatus activeStatus =
                    findFileStatus(sourceFs.listStatus(srcDir.toString()), "active.log");

            HadoopFileSystemProxy sourceFsSpy = Mockito.spy(sourceFs);
            Mockito.doReturn(new FileStatus[] {failedStatus, activeStatus})
                    .doCallRealMethod()
                    .when(sourceFsSpy)
                    .listStatus(srcDir.toString());
            Mockito.doThrow(new IllegalStateException("failed to inspect file"))
                    .doCallRealMethod()
                    .when(sourceFsSpy)
                    .getInputStream(failedStatus.getPath().toString());
            setTableScanContextFileSystem(enumerator, "sourceFs", sourceFsSpy);

            enumerator.scanOnceForTest();
            enumerator.scanOnceForTest();

            Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());

            Files.write(failedFile, "new\n".getBytes(), StandardOpenOption.APPEND);
            enumerator.scanOnceForTest();

            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals(
                    Files.size(failedFile) - "new\n".getBytes().length, split.getStart());
            Assertions.assertEquals("new\n".getBytes().length, split.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testContinuousDiscoveryRetriesAfterScanRuntimeFailure() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("scan_runtime_retry"));
        Path dstDir = Files.createDirectories(tempDir.resolve("scan_runtime_retry_dst"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "content".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir);
        try {
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                    enumeratorWithContext.enumerator;
            HadoopFileSystemProxy sourceFs = getTableScanContextFileSystem(enumerator, "sourceFs");
            FileStatus[] statuses = sourceFs.listStatus(srcDir.toString());
            HadoopFileSystemProxy sourceFsSpy = Mockito.spy(sourceFs);
            Mockito.doThrow(new IllegalStateException("scan failed"))
                    .doReturn(statuses)
                    .when(sourceFsSpy)
                    .listStatus(srcDir.toString());
            setTableScanContextFileSystem(enumerator, "sourceFs", sourceFsSpy);

            Assertions.assertDoesNotThrow(enumerator::safeScanOnce);
            enumerator.safeScanOnce();

            Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingDoesNotCommitDeletedInFlightRange() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_deleted_inflight"));
        Path srcFile = srcDir.resolve("application.log");
        Files.write(srcFile, "first\n".getBytes());

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Files.delete(srcFile);
            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(split.splitId(), null, 0L));

            FileSourceState state = enumeratorWithContext.enumerator.snapshotState(1L);
            Assertions.assertEquals(
                    0L, state.getFileTailStates().values().iterator().next().getCommittedOffset());

            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertTrue(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(2L)
                            .getFileTailStates()
                            .isEmpty());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingRestoresInFlightRangeAfterRotation() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_restore_rotation"));
        Path activeFile = srcDir.resolve("application.log");
        Path rotatedFile = srcDir.resolve("application.log.1");
        Files.write(activeFile, "first\n".getBytes());

        FileSourceState checkpointState;
        FileSourceSplit originalSplit;
        EnumeratorWithContext first =
                createTextTailingEnumerator(
                        srcDir, "earliest", new FileSourceState(Collections.emptySet()));
        try {
            first.enumerator.scanOnceForTest();
            originalSplit = assignAndCaptureSingleSplit(first);
            checkpointState = first.enumerator.snapshotState(1L);
        } finally {
            first.enumerator.close();
        }

        Files.write(activeFile, "last-old\n".getBytes(), StandardOpenOption.APPEND);
        Files.move(activeFile, rotatedFile);
        Files.write(activeFile, "first-new\n".getBytes());

        EnumeratorWithContext restored =
                createTextTailingEnumerator(srcDir, "earliest", checkpointState);
        try {
            restored.enumerator.scanOnceForTest();
            List<FileSourceSplit> assigned = assignAndCaptureSplits(restored);
            FileSourceSplit staleRecoveredSplit =
                    assigned.stream()
                            .filter(
                                    split ->
                                            originalSplit
                                                    .getFileIdentity()
                                                    .equals(split.getFileIdentity()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            restored.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(staleRecoveredSplit.splitId(), null, 0L));

            restored.enumerator.scanOnceForTest();
            FileSourceSplit rotatedSplit = assignAndCaptureSingleSplit(restored);
            Assertions.assertTrue(rotatedSplit.getFilePath().endsWith("application.log.1"));
            Assertions.assertEquals(0L, rotatedSplit.getStart());
            Assertions.assertEquals(Files.size(rotatedFile), rotatedSplit.getLength());
        } finally {
            restored.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingSupportsRepeatedPrefixDelimiter() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_delimiter"));
        Files.write(srcDir.resolve("application.log"), "oneababtwoababpartial".getBytes());
        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.ROW_DELIMITER.key(), "abab");

        EnumeratorWithContext enumeratorWithContext =
                createTextTailingEnumerator(
                        srcDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            FileSourceSplit split = assignAndCaptureSingleSplit(enumeratorWithContext);
            Assertions.assertEquals("oneababtwoabab".getBytes().length, split.getLength());
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testLocalTextTailingRejectsEmptyDelimiterAndNonUtf8Encoding() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("text_tail_validation"));
        Map<String, Object> emptyDelimiter = new HashMap<>();
        emptyDelimiter.put(FileBaseSourceOptions.ROW_DELIMITER.key(), "");
        FileConnectorException delimiterException =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                createTextTailingEnumerator(
                                        srcDir,
                                        "earliest",
                                        new FileSourceState(Collections.emptySet()),
                                        emptyDelimiter));
        Assertions.assertTrue(delimiterException.getMessage().contains("row_delimiter"));

        Map<String, Object> utf16Encoding = new HashMap<>();
        utf16Encoding.put(FileBaseSourceOptions.ENCODING.key(), "UTF-16");
        FileConnectorException encodingException =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                createTextTailingEnumerator(
                                        srcDir,
                                        "earliest",
                                        new FileSourceState(Collections.emptySet()),
                                        utf16Encoding));
        Assertions.assertTrue(encodingException.getMessage().contains("encoding=UTF-8"));
    }

    @Test
    void testLocalTextTailingReportsMissingSourcePath() {
        Path missingPath = tempDir.resolve("missing_text_tail_path");

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                createTextTailingEnumerator(
                                        missingPath,
                                        "earliest",
                                        new FileSourceState(Collections.emptySet())));

        Assertions.assertTrue(exception.getMessage().contains("path does not exist"));
        Assertions.assertTrue(exception.getMessage().contains(missingPath.toString()));
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

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.SCAN_INTERVAL.key(), Duration.ZERO);

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("scan_interval > 0"),
                "continuous mode should require a positive scan_interval");
    }

    @Test
    void testContinuousDiscoveryBackupActionRequiresBackupPath() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src9"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst9"));

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("post_sync_action=backup requires backup_path"),
                "backup action should require backup_path");
    }

    @Test
    void testContinuousDiscoveryBackupActionRejectsCrossFileSystemBackupPath() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src9_cross_fs"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst9_cross_fs"));

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        config.put(FileBaseSourceOptions.BACKUP_PATH.key(), "hdfs://cluster-b/backup");

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("same-filesystem backup"),
                "backup action should reject cross-file-system backup_path in phase-1");
    }

    @Test
    void testContinuousDiscoveryBackupActionRejectsOverlappingBackupPath() throws Exception {
        Path rootDir = Files.createDirectories(tempDir.resolve("overlap_root"));
        Path srcDir = Files.createDirectories(rootDir.resolve("src"));
        Path dstDir = Files.createDirectories(rootDir.resolve("dst"));

        Map<String, Object> nestedBackupConfig = baseContinuousConfig(srcDir, dstDir);
        nestedBackupConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        nestedBackupConfig.put(
                FileBaseSourceOptions.BACKUP_PATH.key(), srcDir.resolve("backup").toString());

        FileConnectorException nestedBackupException =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                createValidationEnumerator(
                                        ReadonlyConfig.fromMap(nestedBackupConfig)));
        Assertions.assertTrue(
                nestedBackupException.getMessage().contains("must not overlap with path"),
                "backup_path under source path should be rejected");

        Map<String, Object> parentBackupConfig = baseContinuousConfig(srcDir, dstDir);
        parentBackupConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        parentBackupConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), rootDir.toString());

        FileConnectorException parentBackupException =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                createValidationEnumerator(
                                        ReadonlyConfig.fromMap(parentBackupConfig)));
        Assertions.assertTrue(
                parentBackupException.getMessage().contains("must not overlap with path"),
                "source path under backup_path should be rejected");
    }

    @Test
    void testContinuousDiscoveryBackupActionRejectsSymlinkedOverlappingBackupPath()
            throws Exception {
        Path rootDir = Files.createDirectories(tempDir.resolve("symlink_overlap_root"));
        Path srcDir = Files.createDirectories(rootDir.resolve("src"));
        Path dstDir = Files.createDirectories(rootDir.resolve("dst"));
        Path rootAlias = tempDir.resolve("symlink_overlap_alias");
        Files.createSymbolicLink(rootAlias, rootDir);

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        config.put(
                FileBaseSourceOptions.BACKUP_PATH.key(),
                rootAlias.resolve("src").resolve("backup").toString());

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("must not overlap with path"),
                "a symlink alias under the source path must be rejected");
    }

    @Test
    void testContinuousDiscoveryPostSyncActionRejectsFilesystemRoot() throws Exception {
        Path dstDir = Files.createDirectories(tempDir.resolve("root_path_dst"));

        Map<String, Object> config = baseContinuousConfig(java.nio.file.Paths.get("/"), dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("non-root directory"),
                "post-sync actions must reject the filesystem root");
    }

    @Test
    void testContinuousDiscoveryDeleteActionRejectsBackupPath() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src10"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst10"));

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        config.put(FileBaseSourceOptions.BACKUP_PATH.key(), dstDir.resolve("backup").toString());

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("backup_path is only valid when post_sync_action=backup"),
                "backup_path should be rejected unless action is backup");
    }

    @Test
    void testContinuousDiscoveryRetentionRequiresBackupAction() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src11"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst11"));

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        config.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "1H");

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("retention_max_age is only valid when post_sync_action=backup"),
                "retention_max_age should require backup action");
    }

    @Test
    void testContinuousDiscoveryRetentionRequiresPositiveCheckInterval() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src12"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst12"));

        Map<String, Object> config = baseContinuousConfig(srcDir, dstDir);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        config.put(FileBaseSourceOptions.BACKUP_PATH.key(), dstDir.resolve("backup").toString());
        config.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "1H");
        config.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "0S");

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> createValidationEnumerator(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("retention_check_interval must be greater than 0"),
                "retention_check_interval should be positive when retention is enabled");
    }

    @Test
    void testContinuousDiscoveryNoneIgnoresStalePostSyncOptions() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src_none_stale_options"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst_none_stale_options"));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "none");
        extraConfig.put(
                FileBaseSourceOptions.BACKUP_PATH.key(),
                tempDir.resolve("unused_backup_path").toString());
        extraConfig.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "7D");
        extraConfig.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "1H");

        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        enumeratorWithContext.enumerator.close();
    }

    @Test
    void testSnapshotKeepsInFlightSplitWhilePostSyncOperationIsBuilt() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src_atomic_completion"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst_atomic_completion"));
        Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        ExecutorService completionExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch statusReadStarted = new CountDownLatch(1);
        CountDownLatch allowStatusRead = new CountDownLatch(1);
        Future<?> completionFuture = null;
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.handleSplitRequest(0);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);

            Field inFlightContextsField =
                    ContinuousMultipleTableFileSourceSplitEnumerator.class.getDeclaredField(
                            "inFlightSplitContexts");
            inFlightContextsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Object> inFlightContexts =
                    (Map<String, Object>)
                            inFlightContextsField.get(enumeratorWithContext.enumerator);
            Object inFlightContext = inFlightContexts.get(assigned.splitId());
            Field splitVersionField = inFlightContext.getClass().getDeclaredField("splitVersion");
            splitVersionField.setAccessible(true);
            splitVersionField.set(inFlightContext, null);

            Field tableScanContextsField =
                    ContinuousMultipleTableFileSourceSplitEnumerator.class.getDeclaredField(
                            "tableScanContexts");
            tableScanContextsField.setAccessible(true);
            List<?> tableScanContexts =
                    (List<?>) tableScanContextsField.get(enumeratorWithContext.enumerator);
            Object tableScanContext = tableScanContexts.get(0);
            Field sourceFsField = tableScanContext.getClass().getDeclaredField("sourceFs");
            sourceFsField.setAccessible(true);
            HadoopFileSystemProxy sourceFs =
                    (HadoopFileSystemProxy) sourceFsField.get(tableScanContext);
            HadoopFileSystemProxy blockingSourceFs = Mockito.spy(sourceFs);
            Mockito.doAnswer(
                            invocation -> {
                                statusReadStarted.countDown();
                                if (!allowStatusRead.await(30, TimeUnit.SECONDS)) {
                                    throw new AssertionError(
                                            "timed out waiting to resume split version lookup");
                                }
                                return invocation.callRealMethod();
                            })
                    .when(blockingSourceFs)
                    .getFileStatus(Mockito.anyString());
            sourceFsField.set(tableScanContext, blockingSourceFs);

            completionFuture =
                    completionExecutor.submit(
                            () ->
                                    enumeratorWithContext.enumerator.handleSourceEvent(
                                            0, new FileSplitFinishedEvent(assigned.splitId())));

            Assertions.assertTrue(
                    statusReadStarted.await(30, TimeUnit.SECONDS),
                    "split completion should reach the blocked version lookup");
            FileSourceState transitionSnapshot = enumeratorWithContext.enumerator.snapshotState(1L);

            Assertions.assertTrue(
                    transitionSnapshot.getAssignedSplit().contains(assigned),
                    "a snapshot taken while the operation is built must retain the in-flight split");
            Assertions.assertTrue(
                    transitionSnapshot.getPendingOpsByCheckpoint().isEmpty(),
                    "the operation must not be checkpointed before it is fully built");
        } finally {
            allowStatusRead.countDown();
            if (completionFuture != null) {
                completionFuture.get(30, TimeUnit.SECONDS);
            }
            completionExecutor.shutdownNow();
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncDeleteCommittedAfterCheckpointComplete() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src13"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst13"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.handleSplitRequest(0);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);

            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(assigned.splitId()));
            Assertions.assertTrue(
                    Files.exists(srcFile), "source file should exist before checkpoint complete");

            FileSourceState state = enumeratorWithContext.enumerator.snapshotState(1L);
            Assertions.assertEquals(
                    1,
                    state.getPendingOpsByCheckpoint().get(1L).size(),
                    "checkpoint should persist staged post-sync operation");

            Files.write(dstDir.resolve("test.bin"), "abc".getBytes());
            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);
            Assertions.assertFalse(
                    Files.exists(srcFile),
                    "source file should be deleted after checkpoint complete");
            try (java.util.stream.Stream<Path> remainingPaths = Files.list(srcDir)) {
                Assertions.assertEquals(
                        0L,
                        remainingPaths.count(),
                        "post-sync delete should not leave staging directories");
            }
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testRestorePendingPostSyncOperationSuppressesRediscovery() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src13_restore_pending_operation"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst13_restore_pending_operation"));
        Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        EnumeratorWithContext first =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            stageSinglePostSyncOperation(first, 1L);
            FileSourceState checkpointState = first.enumerator.snapshotState(2L);

            EnumeratorWithContext restored =
                    createEnumerator(srcDir, dstDir, "earliest", checkpointState, extraConfig);
            try {
                restored.enumerator.scanOnceForTest();
                Assertions.assertEquals(
                        0,
                        restored.enumerator.currentUnassignedSplitSize(),
                        "restored post-sync operations must suppress re-discovery of the same source version");
            } finally {
                restored.enumerator.close();
            }
        } finally {
            first.enumerator.close();
        }
    }

    @Test
    void testPostSyncValidationClosesTemporaryFileSystems() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src13_validation_close"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst13_validation_close"));
        Path backupDir = tempDir.resolve("backup13_validation_close");
        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());

        try (MockedConstruction<HadoopFileSystemProxy> mockedFileSystems =
                Mockito.mockConstruction(
                        HadoopFileSystemProxy.class,
                        (mock, context) ->
                                Mockito.when(mock.makeQualifiedPath(Mockito.anyString()))
                                        .thenAnswer(invocation -> invocation.getArgument(0)))) {
            EnumeratorWithContext enumeratorWithContext =
                    createEnumerator(
                            srcDir,
                            dstDir,
                            "earliest",
                            new FileSourceState(Collections.emptySet()),
                            extraConfig);
            try {
                List<HadoopFileSystemProxy> constructed = mockedFileSystems.constructed();
                Assertions.assertTrue(constructed.size() >= 2);
                Mockito.verify(constructed.get(0)).close();
                Mockito.verify(constructed.get(1)).close();
            } finally {
                enumeratorWithContext.enumerator.close();
            }
        }
    }

    @Test
    void testPostSyncDeleteVersionGuardSkipsStaleOperation() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src13_stale_delete"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst13_stale_delete"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.handleSplitRequest(0);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);

            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(assigned.splitId()));
            enumeratorWithContext.enumerator.snapshotState(1L);

            Files.write(srcFile, "abcd".getBytes());
            Files.setLastModifiedTime(
                    srcFile, FileTime.fromMillis(System.currentTimeMillis() + 5_000));

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(srcFile),
                    "new source version should not be deleted by stale post-sync delete operation");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncDeleteSkipsStaleOperationWhenSourceContentChangesWithoutVersionDrift()
            throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src13_same_version"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst13_same_version"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        byte[] originalContent = "abc".getBytes();
        byte[] recreatedContent = "xyz".getBytes();
        Files.write(srcFile, originalContent);
        Files.write(dstFile, originalContent);
        long baseTime = System.currentTimeMillis();
        Files.setLastModifiedTime(dstFile, FileTime.fromMillis(baseTime - 5_000));
        Files.setLastModifiedTime(srcFile, FileTime.fromMillis(baseTime));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Files.write(srcFile, recreatedContent);
            Files.setLastModifiedTime(
                    srcFile, FileTime.fromMillis(operation.getSourceModificationTime()));

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(srcFile),
                    "stale delete should keep the recreated source file in place");
            Assertions.assertArrayEquals(recreatedContent, Files.readAllBytes(srcFile));
            Assertions.assertFalse(
                    Files.exists(
                            java.nio.file.Paths.get(
                                    new org.apache.hadoop.fs.Path(
                                                    ContinuousMultipleTableFileSourceSplitEnumerator
                                                            .buildDeleteStagingPath(operation, 1L))
                                            .toUri())),
                    "stale delete should not leave behind a staged trash file");
            Assertions.assertFalse(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(2L)
                            .getPendingOpsByCheckpoint()
                            .containsKey(1L),
                    "stale same-version delete should be skipped instead of retried forever");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupStagesTargetOnSourceFileSystem() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_qualified_backup"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_qualified_backup"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_qualified_backup"));
        Files.write(srcDir.resolve("test.bin"), "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.handleSplitRequest(0);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);
            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(assigned.splitId()));

            FileSourceOperationState operation =
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(1L)
                            .getPendingOpsByCheckpoint()
                            .get(1L)
                            .get(0);
            org.apache.hadoop.fs.Path backupTargetPath =
                    new org.apache.hadoop.fs.Path(operation.getBackupTargetPath());
            Assertions.assertEquals("file", backupTargetPath.toUri().getScheme());
            Assertions.assertTrue(
                    backupTargetPath.toUri().getPath().startsWith(backupDir.toString()),
                    "backup target should remain on the source filesystem");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupVersionGuardSkipsStaleOperation() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            enumeratorWithContext.enumerator.handleSplitRequest(0);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                    ArgumentCaptor.forClass((Class) List.class);
            Mockito.verify(enumeratorWithContext.context)
                    .assignSplit(Mockito.eq(0), splitsCaptor.capture());
            FileSourceSplit assigned = splitsCaptor.getValue().get(0);
            enumeratorWithContext.enumerator.handleSourceEvent(
                    0, new FileSplitFinishedEvent(assigned.splitId()));
            enumeratorWithContext.enumerator.snapshotState(1L);

            // Simulate source file updated before post-sync commit.
            Files.write(srcFile, "abcd".getBytes());
            Files.setLastModifiedTime(
                    srcFile, FileTime.fromMillis(System.currentTimeMillis() + 5_000));

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(srcFile),
                    "new source version should not be moved by stale post-sync backup operation");
            long backupFileCount;
            try (java.util.stream.Stream<Path> stream = Files.walk(backupDir)) {
                backupFileCount = stream.filter(Files::isRegularFile).count();
            }
            Assertions.assertEquals(0, backupFileCount, "stale backup operation should be skipped");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupWaitsForSinkTargetBeforeStagingSource() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_wait_for_target"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_wait_for_target"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_wait_for_target"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());

            HadoopFileSystemProxy sourceFs =
                    getTableScanContextFileSystem(enumeratorWithContext.enumerator, "sourceFs");
            HadoopFileSystemProxy sourceFsSpy = Mockito.spy(sourceFs);
            setTableScanContextFileSystem(
                    enumeratorWithContext.enumerator, "sourceFs", sourceFsSpy);

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(srcFile),
                    "backup should keep the source visible while the sink target is not committed");
            Assertions.assertFalse(
                    Files.exists(backupTarget),
                    "backup target should not be published before the sink target is committed");
            Assertions.assertTrue(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(2L)
                            .getPendingOpsByCheckpoint()
                            .containsKey(1L),
                    "backup operation should remain pending until the sink target is committed");
            Mockito.verify(sourceFsSpy, Mockito.never())
                    .renameFile(
                            Mockito.eq(operation.getSourcePath()),
                            Mockito.anyString(),
                            Mockito.eq(false));

            Files.write(dstFile, "abc".getBytes());
            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertFalse(
                    Files.exists(srcFile),
                    "backup commit should move the source after the sink target is committed");
            Assertions.assertTrue(
                    Files.exists(backupTarget),
                    "backup target should be created after the sink target is committed");
            Assertions.assertFalse(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(3L)
                            .getPendingOpsByCheckpoint()
                            .containsKey(1L),
                    "successful backup commit should clear the pending checkpoint operation");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupSkipsStaleOperationWhenSourceContentChangesWithoutVersionDrift()
            throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_same_version"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_same_version"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_same_version"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        byte[] originalContent = "abc".getBytes();
        byte[] recreatedContent = "xyz".getBytes();
        Files.write(srcFile, originalContent);
        Files.write(dstFile, originalContent);
        long baseTime = System.currentTimeMillis();
        Files.setLastModifiedTime(dstFile, FileTime.fromMillis(baseTime - 5_000));
        Files.setLastModifiedTime(srcFile, FileTime.fromMillis(baseTime));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Files.write(srcFile, recreatedContent);
            Files.setLastModifiedTime(
                    srcFile, FileTime.fromMillis(operation.getSourceModificationTime()));

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(srcFile), "stale backup should restore the recreated source file");
            Assertions.assertArrayEquals(recreatedContent, Files.readAllBytes(srcFile));
            Assertions.assertFalse(
                    Files.exists(
                            java.nio.file.Paths.get(
                                    new org.apache.hadoop.fs.Path(operation.getBackupTargetPath())
                                            .toUri())),
                    "stale backup should not publish a backup file for the wrong content");
            Assertions.assertFalse(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(2L)
                            .getPendingOpsByCheckpoint()
                            .containsKey(1L),
                    "stale same-version backup should be skipped instead of retried forever");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupDoesNotSkipWhenSourceMtimeDriftsButContentStillMatchesTarget()
            throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_source_mtime_drift"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_source_mtime_drift"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_source_mtime_drift"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());
        Files.write(dstFile, "abc".getBytes());

        long baseTime = System.currentTimeMillis();
        Files.setLastModifiedTime(dstFile, FileTime.fromMillis(baseTime - 5_000));
        Files.setLastModifiedTime(srcFile, FileTime.fromMillis(baseTime));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            HadoopFileSystemProxy sourceFs =
                    getTableScanContextFileSystem(enumeratorWithContext.enumerator, "sourceFs");
            HadoopFileSystemProxy sourceFsWithDriftedMtime = Mockito.spy(sourceFs);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());

            Mockito.doAnswer(
                            invocation -> {
                                String filePath = invocation.getArgument(0);
                                FileStatus status = (FileStatus) invocation.callRealMethod();
                                if (!operation.getSourcePath().equals(filePath) || status == null) {
                                    return status;
                                }
                                return new FileStatus(
                                        status.getLen(),
                                        status.isDirectory(),
                                        status.getReplication(),
                                        status.getBlockSize(),
                                        status.getModificationTime() + 60_000,
                                        status.getAccessTime(),
                                        status.getPermission(),
                                        status.getOwner(),
                                        status.getGroup(),
                                        status.getPath());
                            })
                    .when(sourceFsWithDriftedMtime)
                    .getFileStatus(Mockito.anyString());
            setTableScanContextFileSystem(
                    enumeratorWithContext.enumerator, "sourceFs", sourceFsWithDriftedMtime);

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertFalse(
                    Files.exists(srcFile),
                    "backup commit should not be skipped when unchanged source content only reports a drifted mtime");
            Assertions.assertTrue(
                    Files.exists(backupTarget),
                    "backup target should still be created when the current source content matches the committed target");
            FileSourceState state = enumeratorWithContext.enumerator.snapshotState(2L);
            Assertions.assertFalse(
                    state.getPendingOpsByCheckpoint().containsKey(1L),
                    "successful backup commit should clear the pending checkpoint operation even when source mtime drifts");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncDeleteCompletesStagedOperationAfterRestore() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_staged_delete"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_staged_delete"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "delete");
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Path trashPath =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(
                                            ContinuousMultipleTableFileSourceSplitEnumerator
                                                    .buildDeleteStagingPath(operation, 1L))
                                    .toUri());
            Files.createDirectories(trashPath.getParent());
            Files.move(srcFile, trashPath, StandardCopyOption.ATOMIC_MOVE);
            Files.write(dstFile, "abc".getBytes());

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertFalse(
                    Files.exists(trashPath),
                    "a restored operation must delete its already-staged source file");
            Assertions.assertFalse(
                    Files.exists(srcFile), "source should remain absent after cleanup");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupLeavesRecreatedSourceWhenTargetAlreadyExists() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_existing_backup"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_existing_backup"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_existing_backup"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());
            Files.createDirectories(backupTarget.getParent());
            Files.write(backupTarget, "abc".getBytes());

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(srcFile),
                    "an existing backup target must never cause direct deletion of a recreated source");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupRecognizesCompletedRenameAfterRestore() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_restore_backup"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_restore_backup"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_restore_backup"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());
            Files.createDirectories(backupTarget.getParent());
            Files.move(srcFile, backupTarget, StandardCopyOption.ATOMIC_MOVE);

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            FileSourceState state = enumeratorWithContext.enumerator.snapshotState(2L);
            Assertions.assertFalse(
                    state.getPendingOpsByCheckpoint().containsKey(1L),
                    "a matching destination after restore must complete the pending backup operation");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupDoesNotRollbackWhenRenameChangesBackupMtime() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_backup_mtime"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_backup_mtime"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup14_backup_mtime"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        Files.write(dstFile, "abc".getBytes());
        Files.write(srcFile, "abc".getBytes());
        long baseTime = System.currentTimeMillis();
        Files.setLastModifiedTime(dstFile, FileTime.fromMillis(baseTime - 5_000));
        Files.setLastModifiedTime(srcFile, FileTime.fromMillis(baseTime));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            HadoopFileSystemProxy sourceFs =
                    getTableScanContextFileSystem(enumeratorWithContext.enumerator, "sourceFs");
            HadoopFileSystemProxy mtimeMutatingSourceFs = Mockito.spy(sourceFs);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());
            Mockito.doAnswer(
                            invocation -> {
                                invocation.callRealMethod();
                                String newFilePath = invocation.getArgument(1);
                                Path renamedPath =
                                        java.nio.file.Paths.get(
                                                new org.apache.hadoop.fs.Path(newFilePath).toUri());
                                if (renamedPath.startsWith(backupDir)) {
                                    Files.setLastModifiedTime(
                                            renamedPath,
                                            FileTime.fromMillis(
                                                    System.currentTimeMillis() + 60_000));
                                }
                                return null;
                            })
                    .when(mtimeMutatingSourceFs)
                    .renameFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean());
            setTableScanContextFileSystem(
                    enumeratorWithContext.enumerator, "sourceFs", mtimeMutatingSourceFs);
            setTableScanContextFileSystem(
                    enumeratorWithContext.enumerator, "targetFs", mtimeMutatingSourceFs);

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertFalse(
                    Files.exists(srcFile),
                    "backup commit should not restore the source file when only backup mtime changes");
            Assertions.assertTrue(
                    Files.exists(backupTarget),
                    "backup target should remain in place after a successful backup commit");
            FileSourceState state = enumeratorWithContext.enumerator.snapshotState(2L);
            Assertions.assertFalse(
                    state.getPendingOpsByCheckpoint().containsKey(1L),
                    "successful backup commit should clear the pending checkpoint operation");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupRetainsInconsistentRestoreForRetry() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src14_inconsistent_restore_backup"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst14_inconsistent_restore_backup"));
        Path backupDir =
                Files.createDirectories(tempDir.resolve("backup14_inconsistent_restore_backup"));
        Path srcFile = srcDir.resolve("test.bin");
        Files.write(srcFile, "abc".getBytes());

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());
            Files.createDirectories(backupTarget.getParent());
            Files.move(srcFile, backupTarget, StandardCopyOption.ATOMIC_MOVE);
            Files.write(backupTarget, "unexpected-content".getBytes());

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            FileSourceState state = enumeratorWithContext.enumerator.snapshotState(2L);
            Assertions.assertTrue(
                    state.getPendingOpsByCheckpoint().containsKey(1L),
                    "an inconsistent restored backup must remain pending instead of being accepted");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testPostSyncOperationRetryWhenContextMissing() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src15"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst15"));

        FileSourceOperationState operationState =
                new FileSourceOperationState(
                        "unknown_table",
                        "unknown_table_missing",
                        srcDir.resolve("missing.bin").toString(),
                        1L,
                        1L,
                        FilePostSyncAction.DELETE,
                        null);

        Map<Long, List<FileSourceOperationState>> pendingByCheckpoint = new HashMap<>();
        pendingByCheckpoint.put(1L, new ArrayList<>(Collections.singletonList(operationState)));
        FileSourceState checkpointState =
                new FileSourceState(
                        Collections.emptySet(),
                        System.currentTimeMillis(),
                        pendingByCheckpoint,
                        new HashMap<>());

        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir, dstDir, "earliest", checkpointState, Collections.emptyMap());
        try {
            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);
            FileSourceState stateAfterRetry = enumeratorWithContext.enumerator.snapshotState(2L);

            Assertions.assertTrue(
                    stateAfterRetry.getPendingOpsByCheckpoint().containsKey(1L),
                    "failed post-sync operation should remain pending for retry");
            Assertions.assertEquals(
                    1,
                    stateAfterRetry.getPendingOpsByCheckpoint().get(1L).size(),
                    "pending operation should remain after failed commit");
            Assertions.assertTrue(
                    stateAfterRetry.getPendingOpsByCheckpoint().get(1L).get(0).getRetryCount() >= 1,
                    "retry counter should increase after failed commit");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testRetentionKeepsFreshBackupEvenWhenRenamePreservesAnExpiredSourceMtime()
            throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src16_fresh_backup"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst16_fresh_backup"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup16_fresh_backup"));
        Path srcFile = srcDir.resolve("test.bin");
        Path dstFile = dstDir.resolve("test.bin");
        byte[] content = "abc".getBytes();
        Files.write(srcFile, content);
        Files.write(dstFile, content);
        long expiredSourceMtime = System.currentTimeMillis() - 60_000;
        Files.setLastModifiedTime(dstFile, FileTime.fromMillis(expiredSourceMtime - 5_000));
        Files.setLastModifiedTime(srcFile, FileTime.fromMillis(expiredSourceMtime));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        extraConfig.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "1S");
        extraConfig.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "1S");

        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            FileSourceOperationState operation =
                    stageSinglePostSyncOperation(enumeratorWithContext, 1L);
            Path backupTarget =
                    java.nio.file.Paths.get(
                            new org.apache.hadoop.fs.Path(operation.getBackupTargetPath()).toUri());
            Assertions.assertTrue(
                    backupTarget.getFileName().toString().matches(".+\\.v\\d+_\\d+_\\d+"),
                    "backup target name should encode the backup creation time");

            HadoopFileSystemProxy sourceFs =
                    getTableScanContextFileSystem(enumeratorWithContext.enumerator, "sourceFs");
            HadoopFileSystemProxy sourceFsWithExpiredBackupMtime = Mockito.spy(sourceFs);
            Mockito.doAnswer(
                            invocation -> {
                                invocation.callRealMethod();
                                String newFilePath = invocation.getArgument(1);
                                Path renamedPath =
                                        java.nio.file.Paths.get(
                                                new org.apache.hadoop.fs.Path(newFilePath).toUri());
                                if (renamedPath.startsWith(backupDir)
                                        && Files.exists(renamedPath)) {
                                    Files.setLastModifiedTime(
                                            renamedPath, FileTime.fromMillis(expiredSourceMtime));
                                }
                                return null;
                            })
                    .when(sourceFsWithExpiredBackupMtime)
                    .renameFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean());
            setTableScanContextFileSystem(
                    enumeratorWithContext.enumerator, "sourceFs", sourceFsWithExpiredBackupMtime);

            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(backupTarget),
                    "retention should not delete a backup created in the current checkpoint");
            Assertions.assertFalse(Files.exists(srcFile), "backup commit should still move source");
            Assertions.assertFalse(
                    enumeratorWithContext
                            .enumerator
                            .snapshotState(2L)
                            .getPendingOpsByCheckpoint()
                            .containsKey(1L),
                    "successful backup commit should clear the pending checkpoint operation");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testRetentionKeepsExpiredFilesWithoutSeaTunnelVersionSuffix() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src16_suffix"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst16_suffix"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup16_suffix"));
        Path expiredManagedFile = backupDir.resolve("source.bin.v3_123456");
        Path expiredUnmanagedFile = backupDir.resolve("unmanaged.bin");
        Files.write(expiredManagedFile, "old-managed".getBytes());
        Files.write(expiredUnmanagedFile, "old-unmanaged".getBytes());
        FileTime expiredTime = FileTime.fromMillis(System.currentTimeMillis() - 60_000);
        Files.setLastModifiedTime(expiredManagedFile, expiredTime);
        Files.setLastModifiedTime(expiredUnmanagedFile, expiredTime);

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        extraConfig.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "1S");
        extraConfig.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "1S");

        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);

            Assertions.assertFalse(
                    Files.exists(expiredManagedFile),
                    "retention should remove expired files created with SeaTunnel version suffix");
            Assertions.assertTrue(
                    Files.exists(expiredUnmanagedFile),
                    "retention should not remove files without SeaTunnel version suffix");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testRetentionDeletesExpiredBackupFiles() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src16"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst16"));
        Path backupDir = Files.createDirectories(tempDir.resolve("backup16"));
        Path expiredFile = backupDir.resolve("expired.bin.v3_123456");
        Files.write(expiredFile, "old".getBytes());
        Files.setLastModifiedTime(
                expiredFile, FileTime.fromMillis(System.currentTimeMillis() - 60_000));

        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        extraConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), backupDir.toString());
        extraConfig.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "1S");
        extraConfig.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "1S");

        EnumeratorWithContext enumeratorWithContext =
                createEnumerator(
                        srcDir,
                        dstDir,
                        "earliest",
                        new FileSourceState(Collections.emptySet()),
                        extraConfig);
        try {
            enumeratorWithContext.enumerator.notifyCheckpointComplete(1L);
            Assertions.assertFalse(
                    Files.exists(expiredFile), "retention should remove expired backup files");
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    private ContinuousMultipleTableFileSourceSplitEnumerator createValidationEnumerator(
            ReadonlyConfig readonlyConfig) {
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

        return new ContinuousMultipleTableFileSourceSplitEnumerator(
                context, multipleTableFileSourceConfig, new DefaultFileSplitStrategy());
    }

    private Map<String, Object> baseContinuousConfig(Path srcDir, Path dstDir) {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), srcDir.toString());
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "binary");
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        config.put(FileBaseSourceOptions.START_MODE.key(), "earliest");
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");
        config.put(FileBaseSourceOptions.TARGET_PATH.key(), dstDir.toString());
        config.put(FileBaseSourceOptions.UPDATE_STRATEGY.key(), "distcp");
        config.put(FileBaseSourceOptions.COMPARE_MODE.key(), "len_mtime");
        return config;
    }

    @Test
    void testContinuousDiscoveryWithNonRecursiveFileScan() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src_recursive_disabled"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst_recursive_disabled"));
        Path rootFile = srcDir.resolve("root.bin");
        Path nestedFile = Files.createDirectories(srcDir.resolve("nested")).resolve("nested.bin");
        Files.write(rootFile, "abc".getBytes());
        Files.write(nestedFile, "def".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir, false);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    1, enumeratorWithContext.enumerator.currentUnassignedSplitSize());

            List<String> filePaths = assignAndCaptureFilePaths(enumeratorWithContext);
            Assertions.assertEquals(1, filePaths.size());
            Assertions.assertTrue(filePaths.get(0).endsWith(rootFile.toString()));
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testContinuousDiscoveryWithDefaultRecursiveFileScan() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src_recursive_default"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst_recursive_default"));
        Path rootFile = srcDir.resolve("root.bin");
        Path nestedFile = Files.createDirectories(srcDir.resolve("nested")).resolve("nested.bin");
        Files.write(rootFile, "abc".getBytes());
        Files.write(nestedFile, "def".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    2, enumeratorWithContext.enumerator.currentUnassignedSplitSize());

            List<String> filePaths = assignAndCaptureFilePaths(enumeratorWithContext);
            Assertions.assertTrue(
                    filePaths.stream().anyMatch(path -> path.endsWith(rootFile.toString())));
            Assertions.assertTrue(
                    filePaths.stream().anyMatch(path -> path.endsWith(nestedFile.toString())));
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    @Test
    void testContinuousDiscoveryWithRecursiveFileScan() throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src_recursive_enabled"));
        Path dstDir = Files.createDirectories(tempDir.resolve("dst_recursive_enabled"));
        Path rootFile = srcDir.resolve("root.bin");
        Path nestedFile = Files.createDirectories(srcDir.resolve("nested")).resolve("nested.bin");
        Files.write(rootFile, "abc".getBytes());
        Files.write(nestedFile, "def".getBytes());

        EnumeratorWithContext enumeratorWithContext = createEnumerator(srcDir, dstDir, true);
        try {
            enumeratorWithContext.enumerator.scanOnceForTest();
            Assertions.assertEquals(
                    2, enumeratorWithContext.enumerator.currentUnassignedSplitSize());

            List<String> filePaths = assignAndCaptureFilePaths(enumeratorWithContext);
            Assertions.assertTrue(
                    filePaths.stream().anyMatch(path -> path.endsWith(rootFile.toString())));
            Assertions.assertTrue(
                    filePaths.stream().anyMatch(path -> path.endsWith(nestedFile.toString())));
        } finally {
            enumeratorWithContext.enumerator.close();
        }
    }

    private EnumeratorWithContext createEnumerator(Path srcDir, Path dstDir) throws IOException {
        return createEnumerator(srcDir, dstDir, "earliest");
    }

    private EnumeratorWithContext createEnumerator(
            Path srcDir, Path dstDir, boolean recursiveFileScan) throws IOException {
        Map<String, Object> extraConfig = new HashMap<>();
        extraConfig.put(FileBaseSourceOptions.RECURSIVE_FILE_SCAN.key(), recursiveFileScan);
        return createEnumerator(
                srcDir,
                dstDir,
                "earliest",
                new FileSourceState(Collections.emptySet()),
                extraConfig);
    }

    private EnumeratorWithContext createEnumerator(Path srcDir, Path dstDir, String startMode)
            throws IOException {
        return createEnumerator(
                srcDir,
                dstDir,
                startMode,
                new FileSourceState(Collections.emptySet()),
                Collections.emptyMap());
    }

    private EnumeratorWithContext createEnumerator(
            Path srcDir, Path dstDir, String startMode, FileSourceState checkpointState)
            throws IOException {
        return createEnumerator(srcDir, dstDir, startMode, checkpointState, Collections.emptyMap());
    }

    private EnumeratorWithContext createEnumerator(
            Path srcDir,
            Path dstDir,
            String startMode,
            FileSourceState checkpointState,
            Map<String, Object> extraConfig)
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
        if (extraConfig != null && !extraConfig.isEmpty()) {
            config.putAll(extraConfig);
        }

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

    private EnumeratorWithContext createTextTailingEnumerator(
            Path srcDir, String startMode, FileSourceState checkpointState) throws IOException {
        return createTextTailingEnumerator(
                srcDir, startMode, checkpointState, Collections.emptyMap());
    }

    private EnumeratorWithContext createTextTailingEnumerator(
            Path srcDir,
            String startMode,
            FileSourceState checkpointState,
            Map<String, Object> extraConfig)
            throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), srcDir.toString());
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "text");
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        config.put(FileBaseSourceOptions.START_MODE.key(), startMode);
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "full");
        config.putAll(extraConfig);

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getBaseFileSourceConfig())
                .thenReturn(ReadonlyConfig.fromMap(config));
        Mockito.when(baseFileSourceConfig.getHadoopConfig())
                .thenReturn(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        Mockito.when(baseFileSourceConfig.getPluginName())
                .thenReturn(FileSystemType.LOCAL.getFileSystemPluginName());

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

    private static FileSourceSplit assignAndCaptureSingleSplit(
            EnumeratorWithContext enumeratorWithContext) {
        enumeratorWithContext.enumerator.handleSplitRequest(0);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                ArgumentCaptor.forClass((Class) List.class);
        Mockito.verify(enumeratorWithContext.context, Mockito.atLeastOnce())
                .assignSplit(Mockito.eq(0), splitsCaptor.capture());
        List<FileSourceSplit> latestAssignment =
                splitsCaptor.getAllValues().get(splitsCaptor.getAllValues().size() - 1);
        Assertions.assertEquals(1, latestAssignment.size());
        return latestAssignment.get(0);
    }

    private static List<FileSourceSplit> assignAndCaptureSplits(
            EnumeratorWithContext enumeratorWithContext) {
        enumeratorWithContext.enumerator.handleSplitRequest(0);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                ArgumentCaptor.forClass((Class) List.class);
        Mockito.verify(enumeratorWithContext.context, Mockito.atLeastOnce())
                .assignSplit(Mockito.eq(0), splitsCaptor.capture());
        return splitsCaptor.getAllValues().get(splitsCaptor.getAllValues().size() - 1);
    }

    private static List<String> assignAndCaptureFilePaths(
            EnumeratorWithContext enumeratorWithContext) {
        enumeratorWithContext.enumerator.handleSplitRequest(0);
        ArgumentCaptor<java.util.List<FileSourceSplit>> splitsCaptor =
                ArgumentCaptor.forClass((Class) java.util.List.class);
        Mockito.verify(enumeratorWithContext.context)
                .assignSplit(Mockito.eq(0), splitsCaptor.capture());
        return splitsCaptor.getValue().stream()
                .map(FileSourceSplit::getFilePath)
                .sorted()
                .collect(Collectors.toList());
    }

    private static FileSourceOperationState stageSinglePostSyncOperation(
            EnumeratorWithContext enumeratorWithContext, long checkpointId) throws IOException {
        enumeratorWithContext.enumerator.scanOnceForTest();
        enumeratorWithContext.enumerator.handleSplitRequest(0);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<FileSourceSplit>> splitsCaptor =
                ArgumentCaptor.forClass((Class) List.class);
        Mockito.verify(enumeratorWithContext.context)
                .assignSplit(Mockito.eq(0), splitsCaptor.capture());
        FileSourceSplit assigned = splitsCaptor.getValue().get(0);
        enumeratorWithContext.enumerator.handleSourceEvent(
                0,
                new FileSplitFinishedEvent(
                        assigned.splitId(),
                        calculateFileFingerprint(
                                java.nio.file.Paths.get(
                                        new org.apache.hadoop.fs.Path(assigned.getFilePath())
                                                .toUri()))));

        return enumeratorWithContext
                .enumerator
                .snapshotState(checkpointId)
                .getPendingOpsByCheckpoint()
                .get(checkpointId)
                .get(0);
    }

    private static String calculateFileFingerprint(Path filePath) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(Files.readAllBytes(filePath));
            return toHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is not supported by this JVM", e);
        }
    }

    private static String toHex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int current = bytes[i] & 0xff;
            encoded[i * 2] = digits[current >>> 4];
            encoded[i * 2 + 1] = digits[current & 0x0f];
        }
        return new String(encoded);
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

    private static HadoopFileSystemProxy getTableScanContextFileSystem(
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator, String fieldName)
            throws ReflectiveOperationException {
        Field tableScanContextsField =
                ContinuousMultipleTableFileSourceSplitEnumerator.class.getDeclaredField(
                        "tableScanContexts");
        tableScanContextsField.setAccessible(true);
        List<?> tableScanContexts = (List<?>) tableScanContextsField.get(enumerator);
        Object tableScanContext = tableScanContexts.get(0);
        Field fileSystemField = tableScanContext.getClass().getDeclaredField(fieldName);
        fileSystemField.setAccessible(true);
        return (HadoopFileSystemProxy) fileSystemField.get(tableScanContext);
    }

    private static FileStatus findFileStatus(FileStatus[] statuses, String fileName) {
        for (FileStatus status : statuses) {
            if (fileName.equals(status.getPath().getName())) {
                return status;
            }
        }
        throw new AssertionError("File status not found: " + fileName);
    }

    private static void setTableScanContextFileSystem(
            ContinuousMultipleTableFileSourceSplitEnumerator enumerator,
            String fieldName,
            HadoopFileSystemProxy fileSystem)
            throws ReflectiveOperationException {
        Field tableScanContextsField =
                ContinuousMultipleTableFileSourceSplitEnumerator.class.getDeclaredField(
                        "tableScanContexts");
        tableScanContextsField.setAccessible(true);
        List<?> tableScanContexts = (List<?>) tableScanContextsField.get(enumerator);
        Object tableScanContext = tableScanContexts.get(0);
        Field fileSystemField = tableScanContext.getClass().getDeclaredField(fieldName);
        fileSystemField.setAccessible(true);
        fileSystemField.set(tableScanContext, fileSystem);
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

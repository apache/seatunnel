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
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceOperationState;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf.LocalConf;

import org.apache.hadoop.fs.FileStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "Hadoop local filesystem path handling is not supported on Windows")
class ContinuousMultipleTableFileSourceSplitEnumeratorPostSyncTest {

    private static final String TABLE_ID =
            TableIdentifier.of("catalog", "db", "table").toTablePath().toString();

    @TempDir private Path tempDir;

    @Test
    void testPostSyncBackupRetriesWhenSourceAndTargetAreTemporarilyMissing() throws Exception {
        Path sourceDir = tempDir.resolve("source");
        Path sourcePath = sourceDir.resolve("temporarily-missing-source.bin");
        Path backupTargetPath = tempDir.resolve("backup/temporarily-missing-backup.bin");
        FileSourceOperationState operation =
                new FileSourceOperationState(
                        TABLE_ID,
                        "temporarily-missing-source",
                        sourcePath.toString(),
                        3L,
                        1L,
                        FilePostSyncAction.BACKUP,
                        backupTargetPath.toString());
        Map<Long, List<FileSourceOperationState>> pendingOperations = new HashMap<>();
        pendingOperations.put(1L, new ArrayList<>(Collections.singletonList(operation)));

        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                createEnumerator(
                        sourceDir,
                        tempDir.resolve("target"),
                        new FileSourceState(
                                Collections.emptySet(),
                                System.currentTimeMillis(),
                                pendingOperations,
                                Collections.emptyMap()));
        try {
            enumerator.notifyCheckpointComplete(1L);

            FileSourceState stateAfterRetry = enumerator.snapshotState(2L);
            Assertions.assertTrue(
                    stateAfterRetry.getPendingOpsByCheckpoint().containsKey(1L),
                    "an ambiguously missing backup operation must remain pending for retry");
            Assertions.assertEquals(
                    1,
                    stateAfterRetry.getPendingOpsByCheckpoint().get(1L).get(0).getRetryCount(),
                    "an ambiguously missing backup operation must increment its retry count");
        } finally {
            enumerator.close();
        }
    }

    @Test
    void testPostSyncBackupWaitsUntilSinkTargetContainsSourceData() throws Exception {
        Path sourceDir = tempDir.resolve("source");
        Path sourcePath = sourceDir.resolve("source.bin");
        Path targetDir = tempDir.resolve("target");
        Path targetPath = targetDir.resolve("source.bin");
        Path backupTargetPath = tempDir.resolve("backup/source.bin.v3_1");
        Files.createDirectories(sourceDir);
        Files.write(sourcePath, "abc".getBytes(StandardCharsets.UTF_8));
        FileStatus sourceStatus;
        try (HadoopFileSystemProxy sourceFs =
                new HadoopFileSystemProxy(new LocalConf(FS_DEFAULT_NAME_DEFAULT))) {
            sourceStatus = sourceFs.getFileStatus(sourcePath.toString());
        }
        FileSourceOperationState operation =
                new FileSourceOperationState(
                        TABLE_ID,
                        "source",
                        sourcePath.toString(),
                        sourceStatus.getLen(),
                        sourceStatus.getModificationTime(),
                        FilePostSyncAction.BACKUP,
                        backupTargetPath.toString());
        Map<Long, List<FileSourceOperationState>> pendingOperations = new HashMap<>();
        pendingOperations.put(1L, new ArrayList<>(Collections.singletonList(operation)));

        ContinuousMultipleTableFileSourceSplitEnumerator enumerator =
                createEnumerator(
                        sourceDir,
                        targetDir,
                        new FileSourceState(
                                Collections.emptySet(),
                                System.currentTimeMillis(),
                                pendingOperations,
                                Collections.emptyMap()));
        try {
            enumerator.notifyCheckpointComplete(1L);

            Assertions.assertTrue(
                    Files.exists(sourcePath),
                    "source must remain until the sink target is durably visible");
            Assertions.assertFalse(Files.exists(backupTargetPath));
            Assertions.assertTrue(
                    enumerator.snapshotState(2L).getPendingOpsByCheckpoint().containsKey(1L));

            Files.createDirectories(targetDir);
            Files.write(targetPath, "abc".getBytes(StandardCharsets.UTF_8));
            enumerator.notifyCheckpointComplete(2L);

            Assertions.assertFalse(Files.exists(sourcePath));
            Assertions.assertTrue(Files.exists(backupTargetPath));
            Assertions.assertFalse(
                    enumerator.snapshotState(3L).getPendingOpsByCheckpoint().containsKey(1L));
        } finally {
            enumerator.close();
        }
    }

    private static ContinuousMultipleTableFileSourceSplitEnumerator createEnumerator(
            Path sourceDir, Path targetDir, FileSourceState checkpointState) {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), sourceDir.toString());
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "binary");
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), "continuous");
        config.put(FileBaseSourceOptions.START_MODE.key(), "earliest");
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");
        config.put(FileBaseSourceOptions.TARGET_PATH.key(), targetDir.toString());
        config.put(FileBaseSourceOptions.UPDATE_STRATEGY.key(), "distcp");
        config.put(FileBaseSourceOptions.COMPARE_MODE.key(), "len_mtime");
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        config.put(FileBaseSourceOptions.BACKUP_PATH.key(), targetDir.resolve("backup").toString());

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getBaseFileSourceConfig())
                .thenReturn(ReadonlyConfig.fromMap(config));
        Mockito.when(baseFileSourceConfig.getHadoopConfig())
                .thenReturn(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        Mockito.when(baseFileSourceConfig.getCatalogTable())
                .thenReturn(
                        CatalogTable.of(
                                TableIdentifier.of("catalog", "db", "table"),
                                null,
                                new HashMap<>(),
                                Collections.emptyList(),
                                null));

        BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(1);

        return new ContinuousMultipleTableFileSourceSplitEnumerator(
                context,
                multipleTableFileSourceConfig,
                new DefaultFileSplitStrategy(),
                checkpointState);
    }
}

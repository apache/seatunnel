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

package org.apache.seatunnel.connectors.seatunnel.hive.commit;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreCatalog;

import org.apache.thrift.TException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class HiveSinkAggregatedCommitter extends FileSinkAggregatedCommitter {
    private final String dbName;
    private final String tableName;
    private final boolean abortDropPartitionMetadata;
    private final org.apache.seatunnel.api.sink.DataSaveMode dataSaveMode;

    /**
     * Guard for overwrite semantics in Flink streaming engine.
     *
     * <p>In streaming mode, {@code commit()} is invoked on every completed checkpoint. For
     * overwrite (DROP_DATA), we must avoid deleting target directories on every checkpoint;
     * otherwise previously committed files will be wiped and only the last checkpoint's files
     * remain.
     *
     * <p>We delete each target directory (partition directory / table directory) at most once per
     * job attempt so that dynamic partitions can still be overwritten when first written.
     */
    private final Set<String> deletedTargetDirectories = ConcurrentHashMap.newKeySet();

    /**
     * Best-effort recovery detection based on the first seen checkpoint id embedded in transaction
     * directory name (e.g. .../T_xxx_0_2 means checkpoint 2).
     *
     * <p>If the first seen checkpoint id is greater than 1, it usually indicates the job is
     * recovering from a previous checkpoint. In that case, deleting the target directories would
     * destroy already committed data that is consistent with the restored state.
     */
    private volatile Long minCheckpointIdSeen = null;

    private final ReadonlyConfig readonlyConfig;
    private final HiveMetaStoreCatalog hiveMetaStore;

    public HiveSinkAggregatedCommitter(
            ReadonlyConfig readonlyConfig, String dbName, String tableName, HadoopConf hadoopConf) {
        super(hadoopConf);
        this.readonlyConfig = readonlyConfig;
        this.hiveMetaStore = HiveMetaStoreCatalog.create(readonlyConfig);
        this.dbName = dbName;
        this.tableName = tableName;
        this.abortDropPartitionMetadata =
                readonlyConfig.get(HiveSinkOptions.ABORT_DROP_PARTITION_METADATA);
        // Normalize overwrite into data_save_mode
        org.apache.seatunnel.api.sink.DataSaveMode configured =
                readonlyConfig.get(
                        org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkOptions
                                .DATA_SAVE_MODE);
        boolean overwrite = readonlyConfig.get(HiveSinkOptions.OVERWRITE);
        this.dataSaveMode =
                overwrite ? org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA : configured;
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        log.info("Aggregated commit infos: {}", aggregatedCommitInfos);
        if (dataSaveMode == org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA) {
            updateMinCheckpointIdSeen(aggregatedCommitInfos);
            if (minCheckpointIdSeen != null && minCheckpointIdSeen > 1) {
                log.info(
                        "DataSaveMode=DROP_DATA: skip deleting target directories before commit."
                                + " Recovery is detected, minCheckpointIdSeen={}",
                        minCheckpointIdSeen);
            } else {
                deleteDirectories(aggregatedCommitInfos);
            }
        }

        List<FileAggregatedCommitInfo> errorCommitInfos = super.commit(aggregatedCommitInfos);
        if (errorCommitInfos.isEmpty()) {
            for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
                Map<String, List<String>> partitionDirAndValuesMap =
                        aggregatedCommitInfo.getPartitionDirAndValuesMap();
                List<String> partitions =
                        partitionDirAndValuesMap.keySet().stream()
                                .map(partition -> partition.replaceAll("\\\\", "/"))
                                .collect(Collectors.toList());
                try {
                    hiveMetaStore.addPartitions(dbName, tableName, partitions);
                    log.info("Add these partitions {}", partitions);
                } catch (TException e) {
                    log.error("Failed to add these partitions {}", partitions, e);
                    errorCommitInfos.add(aggregatedCommitInfo);
                }
            }
        }
        return errorCommitInfos;
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        super.abort(aggregatedCommitInfos);
        if (abortDropPartitionMetadata) {
            for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
                Map<String, List<String>> partitionDirAndValuesMap =
                        aggregatedCommitInfo.getPartitionDirAndValuesMap();
                List<String> partitions =
                        partitionDirAndValuesMap.keySet().stream()
                                .map(partition -> partition.replaceAll("\\\\", "/"))
                                .collect(Collectors.toList());
                try {
                    hiveMetaStore.dropPartitions(dbName, tableName, partitions);
                    log.info("Remove these partitions {}", partitions);
                } catch (TException e) {
                    log.error("Failed to remove these partitions {}", partitions, e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            hiveMetaStore.close();
        } finally {
            super.close();
        }
    }

    /**
     * Deletes the partition directories based on the partition paths stored in the aggregated
     * commit information.
     *
     * <p>This method is invoked during the commit phase when the overwrite option is enabled. It
     * iterates over the partition directories specified in the commit information and deletes the
     * directories from the Hadoop file system.
     *
     * @param aggregatedCommitInfos
     */
    private boolean deleteDirectories(List<FileAggregatedCommitInfo> aggregatedCommitInfos)
            throws IOException {
        if (aggregatedCommitInfos.isEmpty()) {
            return false;
        }

        boolean anyDeleted = false;

        for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap =
                    aggregatedCommitInfo.getTransactionMap();

            // Do not delete if source data is empty
            if (transactionMap.values().stream().allMatch(Map::isEmpty)) {
                log.info("Data source is empty, no directories will be deleted.");
                continue;
            }

            try {
                // Get the first target path from transactionMap
                String targetPath =
                        transactionMap.values().stream()
                                .flatMap(m -> m.values().stream())
                                .findFirst()
                                .orElseThrow(
                                        () -> new IllegalStateException("No target paths found"));

                if (aggregatedCommitInfo.getPartitionDirAndValuesMap().isEmpty()) {
                    // For non-partitioned table, extract and delete table directory
                    // Example: hdfs://hadoop-master1:8020/warehouse/test_overwrite_1/
                    int lastSeparator =
                            Math.max(targetPath.lastIndexOf('/'), targetPath.lastIndexOf('\\'));
                    if (lastSeparator <= 0) {
                        log.warn(
                                "Skip deleting table directory because target path has no separator: {}",
                                targetPath);
                        continue;
                    }
                    String tableDir = targetPath.substring(0, lastSeparator);
                    if (deleteTargetDirectoryOnce(tableDir)) {
                        log.info("Deleted table directory: {}", tableDir);
                        anyDeleted = true;
                    }
                } else {
                    // For partitioned table, extract and delete partition directories
                    // Example:
                    // hdfs://hadoop-master1:8020/warehouse/test_overwrite_partition/age=26/
                    Set<String> partitionDirs =
                            transactionMap.values().stream()
                                    .flatMap(m -> m.values().stream())
                                    .map(
                                            path -> {
                                                int sep =
                                                        Math.max(
                                                                path.lastIndexOf('/'),
                                                                path.lastIndexOf('\\'));
                                                if (sep <= 0) {
                                                    return null;
                                                }
                                                return path.substring(0, sep);
                                            })
                                    .filter(p -> p != null && !p.isEmpty())
                                    .collect(Collectors.toSet());

                    for (String partitionDir : partitionDirs) {
                        if (deleteTargetDirectoryOnce(partitionDir)) {
                            log.info("Deleted partition directory: {}", partitionDir);
                            anyDeleted = true;
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Failed to delete directories", e);
                throw e;
            }
        }

        return anyDeleted;
    }

    private boolean deleteTargetDirectoryOnce(String directory) throws IOException {
        if (directory == null || directory.isEmpty()) {
            return false;
        }

        String normalized = normalizeDirectoryPath(directory);
        if (normalized.isEmpty()) {
            return false;
        }

        if (!deletedTargetDirectories.add(normalized)) {
            return false;
        }

        hadoopFileSystemProxy.deleteFile(directory);
        return true;
    }

    private String normalizeDirectoryPath(String directory) {
        String normalized = directory.replace('\\', '/');
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private void updateMinCheckpointIdSeen(List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        if (aggregatedCommitInfos == null || aggregatedCommitInfos.isEmpty()) {
            return;
        }

        long minInThisCommit = Long.MAX_VALUE;
        boolean found = false;

        for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            if (aggregatedCommitInfo == null || aggregatedCommitInfo.getTransactionMap() == null) {
                continue;
            }
            for (String transactionDir : aggregatedCommitInfo.getTransactionMap().keySet()) {
                long checkpointId = parseCheckpointIdFromTransactionDir(transactionDir);
                if (checkpointId > 0) {
                    minInThisCommit = Math.min(minInThisCommit, checkpointId);
                    found = true;
                }
            }
        }

        if (!found) {
            return;
        }

        if (minCheckpointIdSeen == null) {
            minCheckpointIdSeen = minInThisCommit;
        } else {
            minCheckpointIdSeen = Math.min(minCheckpointIdSeen, minInThisCommit);
        }
    }

    /**
     * Parses checkpoint id from transaction directory.
     *
     * <p>Expected pattern in transaction dir name: .../T_..._<subtaskIndex>_<checkpointId>
     */
    private long parseCheckpointIdFromTransactionDir(String transactionDir) {
        if (transactionDir == null || transactionDir.isEmpty()) {
            return -1;
        }

        String normalized = transactionDir.replace('\\', '/');
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        int lastSlash = normalized.lastIndexOf('/');
        String baseName = lastSlash >= 0 ? normalized.substring(lastSlash + 1) : normalized;
        if (baseName.isEmpty()) {
            return -1;
        }

        int lastUnderscore = baseName.lastIndexOf('_');
        if (lastUnderscore < 0 || lastUnderscore == baseName.length() - 1) {
            return -1;
        }

        String lastToken = baseName.substring(lastUnderscore + 1);
        try {
            return Long.parseLong(lastToken);
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }
}

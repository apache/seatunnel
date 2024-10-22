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
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.thrift.TException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class HiveSinkAggregatedCommitter extends FileSinkAggregatedCommitter {
    private final String dbName;
    private final String tableName;
    private final boolean abortDropPartitionMetadata;
    private final boolean overwrite;

    private final ReadonlyConfig readonlyConfig;

    public HiveSinkAggregatedCommitter(
            ReadonlyConfig readonlyConfig, String dbName, String tableName, HadoopConf hadoopConf) {
        super(hadoopConf);
        this.readonlyConfig = readonlyConfig;
        this.dbName = dbName;
        this.tableName = tableName;
        this.abortDropPartitionMetadata =
                readonlyConfig.get(HiveSinkOptions.ABORT_DROP_PARTITION_METADATA);
        this.overwrite = readonlyConfig.get(HiveSinkOptions.OVERWRITE);
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        log.info("Aggregated commit infos size: {}", aggregatedCommitInfos.size());
        for (FileAggregatedCommitInfo info : aggregatedCommitInfos) {
            log.info("Commit info: {}", info);
        }
        log.info("overwrite: {}", overwrite);
        // delete if overwrite
        if (overwrite) {
            deleteDirectories(aggregatedCommitInfos);
        }

        List<FileAggregatedCommitInfo> errorCommitInfos = super.commit(aggregatedCommitInfos);
        if (errorCommitInfos.isEmpty()) {
            HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(readonlyConfig);
            try {
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
            } finally {
                hiveMetaStore.close();
            }
        }
        return errorCommitInfos;
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        super.abort(aggregatedCommitInfos);
        if (abortDropPartitionMetadata) {
            HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(readonlyConfig);
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
            hiveMetaStore.close();
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
    private void deleteDirectories(List<FileAggregatedCommitInfo> aggregatedCommitInfos)
            throws IOException {
        if (aggregatedCommitInfos.isEmpty()) {
            return;
        }

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
                    String tableDir = targetPath.substring(0, targetPath.lastIndexOf('/'));
                    hadoopFileSystemProxy.deleteFile(tableDir);
                    log.info("Deleted table directory: {}", tableDir);
                } else {
                    // For partitioned table, extract and delete partition directories
                    // Example: hdfs://hadoop-master1:8020/warehouse/test_overwrite_partition/age=26/
                    Set<String> partitionDirs =
                            transactionMap.values().stream()
                                    .flatMap(m -> m.values().stream())
                                    .map(path -> path.substring(0, path.lastIndexOf('/')))
                                    .collect(Collectors.toSet());

                    for (String partitionDir : partitionDirs) {
                        hadoopFileSystemProxy.deleteFile(partitionDir);
                        log.info("Deleted partition directory: {}", partitionDir);
                    }
                }
            } catch (IOException e) {
                log.error("Failed to delete directories", e);
                throw e;
            }
        }
    }
}

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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.thrift.TException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.ABORT_DROP_PARTITION_METADATA;

@Slf4j
public class HiveSinkAggregatedCommitter extends FileSinkAggregatedCommitter {
    private final Config pluginConfig;
    private final String dbName;
    private final String tableName;
    private final boolean abortDropPartitionMetadata;

    public HiveSinkAggregatedCommitter(
            Config pluginConfig, String dbName, String tableName, FileSystemUtils fileSystemUtils) {
        super(fileSystemUtils);
        this.pluginConfig = pluginConfig;
        this.dbName = dbName;
        this.tableName = tableName;
        this.abortDropPartitionMetadata =
                pluginConfig.hasPath(ABORT_DROP_PARTITION_METADATA.key())
                        ? pluginConfig.getBoolean(ABORT_DROP_PARTITION_METADATA.key())
                        : ABORT_DROP_PARTITION_METADATA.defaultValue();
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(pluginConfig);
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
                } catch (AlreadyExistsException e) {
                    log.warn("These partitions {} are already exists", partitions);
                } catch (TException e) {
                    log.error("Failed to add these partitions {}", partitions, e);
                    errorCommitInfos.add(aggregatedCommitInfo);
                }
            }
        }
        hiveMetaStore.close();
        return errorCommitInfos;
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        super.abort(aggregatedCommitInfos);
        if (abortDropPartitionMetadata) {
            HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(pluginConfig);
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
}

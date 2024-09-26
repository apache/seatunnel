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
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveJDBCCatalog;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.thrift.TException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class HiveSinkAggregatedCommitter extends FileSinkAggregatedCommitter {
    private final String dbName;
    private final String tableName;
    private final boolean abortDropPartitionMetadata;

    private final ReadonlyConfig readonlyConfig;

    public HiveSinkAggregatedCommitter(
            ReadonlyConfig readonlyConfig, String dbName, String tableName, HadoopConf hadoopConf) {
        super(hadoopConf);
        this.readonlyConfig = readonlyConfig;
        this.dbName = dbName;
        this.tableName = tableName;
        this.abortDropPartitionMetadata =
                readonlyConfig.get(HiveSinkOptions.ABORT_DROP_PARTITION_METADATA);
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {

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
                        msckRepairTable();
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
            try {
                for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
                    Map<String, List<String>> partitionDirAndValuesMap =
                            aggregatedCommitInfo.getPartitionDirAndValuesMap();
                    List<String> partitions =
                            partitionDirAndValuesMap.keySet().stream()
                                    .map(partition -> partition.replaceAll("\\\\", "/"))
                                    .collect(Collectors.toList());
                    try {
                        hiveMetaStore.dropPartitions(dbName, tableName, partitions);
                        msckRepairTable();
                        log.info("Remove these partitions {}", partitions);
                    } catch (TException e) {
                        log.error("Failed to remove these partitions {}", partitions, e);
                    }
                }
            } finally {
                hiveMetaStore.close();
            }
        }
    }

    private void msckRepairTable() {
        if (!readonlyConfig.getOptional(HiveConfig.HIVE_JDBC_URL).isPresent()) {
            return;
        }
        // run msck repair table command via jdbc in case add partition not successful.
        HiveJDBCCatalog hiveJDBCCatalog = new HiveJDBCCatalog(readonlyConfig);
        hiveJDBCCatalog.open();
        hiveJDBCCatalog.msckRepairTable(dbName, tableName);
        hiveJDBCCatalog.close();
    }
}

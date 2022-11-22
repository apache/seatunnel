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

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class HiveSinkAggregatedCommitter extends FileSinkAggregatedCommitter {
    private final Config pluginConfig;
    private final String dbName;
    private final String tableName;

    public HiveSinkAggregatedCommitter(Config pluginConfig, String dbName, String tableName, HadoopConf hadoopConf) {
        super(hadoopConf);
        this.pluginConfig = pluginConfig;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(pluginConfig);
        List<FileAggregatedCommitInfo> errorCommitInfos = super.commit(aggregatedCommitInfos);
        if (errorCommitInfos.isEmpty()) {
            for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
                Map<String, List<String>> partitionDirAndValuesMap = aggregatedCommitInfo.getPartitionDirAndValuesMap();
                List<String> partitions = partitionDirAndValuesMap.keySet().stream()
                        .map(partition -> partition.replaceAll("\\\\", "/"))
                        .collect(Collectors.toList());
                try {
                    hiveMetaStore.addPartitions(dbName, tableName, partitions);
                    log.info("Add these partitions {}", partitions);
                } catch (TException e) {
                    log.error("Failed to add these partitions {}", partitions);
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
        HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(pluginConfig);
        for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            Map<String, List<String>> partitionDirAndValuesMap = aggregatedCommitInfo.getPartitionDirAndValuesMap();
            List<String> partitions = partitionDirAndValuesMap.keySet().stream()
                    .map(partition -> partition.replaceAll("\\\\", "/"))
                    .collect(Collectors.toList());
            try {
                hiveMetaStore.dropPartitions(dbName, tableName, partitions);
                log.info("Remove these partitions {}", partitions);
            } catch (TException e) {
                log.error("Failed to remove these partitions {}", partitions);
            }
        }
        hiveMetaStore.close();
    }
}

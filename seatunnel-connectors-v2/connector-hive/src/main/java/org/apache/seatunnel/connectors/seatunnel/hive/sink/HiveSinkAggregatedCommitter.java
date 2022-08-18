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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.util.HdfsUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveSinkAggregatedCommitter implements SinkAggregatedCommitter<HiveCommitInfo, HiveAggregatedCommitInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSinkAggregatedCommitter.class);

    @Override
    public List<HiveAggregatedCommitInfo> commit(List<HiveAggregatedCommitInfo> aggregatedCommitInfoList) throws IOException {
        LOGGER.info("=============================agg commit=================================");
        if (CollectionUtils.isEmpty(aggregatedCommitInfoList)) {
            return null;
        }
        List<HiveAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        HiveMetaStoreProxy hiveMetaStoreProxy = new HiveMetaStoreProxy(aggregatedCommitInfoList.get(0).getHiveMetastoreUris());
        HiveMetaStoreClient hiveMetaStoreClient = hiveMetaStoreProxy.getHiveMetaStoreClient();
        try {
            aggregatedCommitInfoList.forEach(aggregateCommitInfo -> {
                try {
                    for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getFileAggregatedCommitInfo().getTransactionMap().entrySet()) {
                        // rollback the file
                        for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                            HdfsUtils.renameFile(mvFileEntry.getKey(), mvFileEntry.getValue(), true);
                        }
                        // delete the transaction dir
                        HdfsUtils.deleteFile(entry.getKey());
                    }
                    // add hive partition
                    aggregateCommitInfo.getFileAggregatedCommitInfo().getPartitionDirAndValsMap().entrySet().forEach(entry -> {
                        Partition part = new Partition();
                        part.setDbName(aggregateCommitInfo.getTable().getDbName());
                        part.setTableName(aggregateCommitInfo.getTable().getTableName());
                        part.setValues(entry.getValue());
                        part.setParameters(new HashMap<>());
                        part.setSd(aggregateCommitInfo.getTable().getSd().deepCopy());
                        part.getSd().setSerdeInfo(aggregateCommitInfo.getTable().getSd().getSerdeInfo());
                        part.getSd().setLocation(aggregateCommitInfo.getTable().getSd().getLocation() + "/" + entry.getKey());
                        try {
                            hiveMetaStoreClient.add_partition(part);
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (Exception e) {
                    LOGGER.error("commit aggregateCommitInfo error ", e);
                    errorAggregatedCommitInfoList.add(aggregateCommitInfo);
                }
            });
        } finally {
            hiveMetaStoreClient.close();
        }

        return errorAggregatedCommitInfoList;
    }

    @Override
    public HiveAggregatedCommitInfo combine(List<HiveCommitInfo> commitInfos) {
        if (CollectionUtils.isEmpty(commitInfos)) {
            return null;
        }
        Map<String, Map<String, String>> aggregateCommitInfo = new HashMap<>();
        Map<String, List<String>> partitionDirAndValsMap = new HashMap<>();
        commitInfos.forEach(commitInfo -> {
            Map<String, String> needMoveFileMap = aggregateCommitInfo.computeIfAbsent(commitInfo.getFileCommitInfo().getTransactionDir(), k -> new HashMap<>());
            needMoveFileMap.putAll(commitInfo.getFileCommitInfo().getNeedMoveFiles());
            Set<Map.Entry<String, List<String>>> entries = commitInfo.getFileCommitInfo().getPartitionDirAndValsMap().entrySet();
            if (!CollectionUtils.isEmpty(entries)) {
                partitionDirAndValsMap.putAll(commitInfo.getFileCommitInfo().getPartitionDirAndValsMap());
            }
        });
        return new HiveAggregatedCommitInfo(
            new FileAggregatedCommitInfo(aggregateCommitInfo, partitionDirAndValsMap),
            commitInfos.get(0).getHiveMetastoreUris(),
            commitInfos.get(0).getTable());
    }

    @Override
    public void abort(List<HiveAggregatedCommitInfo> aggregatedCommitInfoList) throws Exception {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return;
        }
        aggregatedCommitInfoList.stream().forEach(aggregateCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getFileAggregatedCommitInfo().getTransactionMap().entrySet()) {
                    // rollback the file
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        if (HdfsUtils.fileExist(mvFileEntry.getValue()) && !HdfsUtils.fileExist(mvFileEntry.getKey())) {
                            HdfsUtils.renameFile(mvFileEntry.getValue(), mvFileEntry.getKey(), true);
                        }
                    }
                    // delete the transaction dir
                    HdfsUtils.deleteFile(entry.getKey());

                    // The partitions that have been added will be preserved and will not be deleted
                }
            } catch (IOException e) {
                LOGGER.error("abort aggregateCommitInfo error ", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
    }
}

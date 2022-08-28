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

package org.apache.seatunnel.connectors.seatunnel.file.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileSinkAggregatedCommitter2 implements SinkAggregatedCommitter<FileCommitInfo2, FileAggregatedCommitInfo2> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSinkAggregatedCommitter2.class);

    @Override
    public List<FileAggregatedCommitInfo2> commit(List<FileAggregatedCommitInfo2> aggregatedCommitInfo) throws IOException {
        List<FileAggregatedCommitInfo2> errorAggregatedCommitInfoList = new ArrayList<>();
        aggregatedCommitInfo.forEach(aggregateCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getTransactionMap().entrySet()) {
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        // first rename temp file
                        FileSystemUtils.renameFile(mvFileEntry.getKey(), mvFileEntry.getValue(), true);
                    }
                    // second delete transaction directory
                    FileSystemUtils.deleteFile(entry.getKey());
                }
            } catch (Exception e) {
                LOGGER.error("commit aggregateCommitInfo error ", e);
                errorAggregatedCommitInfoList.add(aggregateCommitInfo);
            }
        });
        return errorAggregatedCommitInfoList;
    }

    /**
     * The logic about how to combine commit message.
     *
     * @param commitInfos The list of commit message.
     * @return The commit message after combine.
     */
    @Override
    public FileAggregatedCommitInfo2 combine(List<FileCommitInfo2> commitInfos) {
        if (commitInfos == null || commitInfos.size() == 0) {
            return null;
        }
        Map<String, Map<String, String>> aggregateCommitInfo = new HashMap<>();
        Map<String, List<String>> partitionDirAndValuesMap = new HashMap<>();
        commitInfos.forEach(commitInfo -> {
            Map<String, String> needMoveFileMap = aggregateCommitInfo.computeIfAbsent(commitInfo.getTransactionDir(), k -> new HashMap<>());
            needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
            Set<Map.Entry<String, List<String>>> entries = commitInfo.getPartitionDirAndValuesMap().entrySet();
            if (!CollectionUtils.isEmpty(entries)) {
                partitionDirAndValuesMap.putAll(commitInfo.getPartitionDirAndValuesMap());
            }
        });
        return new FileAggregatedCommitInfo2(aggregateCommitInfo, partitionDirAndValuesMap);
    }

    /**
     * If {@link #commit(List)} failed, this method will be called (**Only** on Spark engine at now).
     *
     * @param aggregatedCommitInfo The list of combine commit message.
     * @throws Exception throw Exception when abort failed.
     */
    @Override
    public void abort(List<FileAggregatedCommitInfo2> aggregatedCommitInfo) throws Exception {
        if (aggregatedCommitInfo == null || aggregatedCommitInfo.size() == 0) {
            return;
        }
        aggregatedCommitInfo.forEach(aggregateCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getTransactionMap().entrySet()) {
                    // rollback the file
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        if (FileSystemUtils.fileExist(mvFileEntry.getValue()) && !FileSystemUtils.fileExist(mvFileEntry.getKey())) {
                            FileSystemUtils.renameFile(mvFileEntry.getValue(), mvFileEntry.getKey(), true);
                        }
                    }
                    // delete the transaction dir
                    FileSystemUtils.deleteFile(entry.getKey());
                }
            } catch (Exception e) {
                LOGGER.error("abort aggregateCommitInfo error ", e);
            }
        });
    }

    /**
     * Close this resource.
     *
     * @throws IOException throw IOException when close failed.
     */
    @Override
    public void close() throws IOException {

    }
}

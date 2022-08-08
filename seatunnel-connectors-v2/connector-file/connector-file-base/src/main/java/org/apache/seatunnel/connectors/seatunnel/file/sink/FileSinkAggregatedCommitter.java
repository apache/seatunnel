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

package org.apache.seatunnel.connectors.seatunnel.file.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystemCommitter;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileSinkAggregatedCommitter implements SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSinkAggregatedCommitter.class);
    private final FileSystemCommitter fileSystemCommitter;

    public FileSinkAggregatedCommitter(@NonNull FileSystemCommitter fileSystemCommitter) {
        this.fileSystemCommitter = fileSystemCommitter;
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(List<FileAggregatedCommitInfo> aggregatedCommitInfoList) throws IOException {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return null;
        }
        List<FileAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        aggregatedCommitInfoList.forEach(aggregateCommitInfo -> {
            try {
                fileSystemCommitter.commitTransaction(aggregateCommitInfo);
            } catch (Exception e) {
                LOGGER.error("commit aggregateCommitInfo error ", e);
                errorAggregatedCommitInfoList.add(aggregateCommitInfo);
            }
        });

        return errorAggregatedCommitInfoList;
    }

    @Override
    public FileAggregatedCommitInfo combine(List<FileCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.size() == 0) {
            return null;
        }
        Map<String, Map<String, String>> aggregateCommitInfo = new HashMap<>();
        Map<String, List<String>> partitionDirAndValsMap = new HashMap<>();
        commitInfos.forEach(commitInfo -> {
            Map<String, String> needMoveFileMap = aggregateCommitInfo.computeIfAbsent(commitInfo.getTransactionDir(), k -> new HashMap<>());
            needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
            Set<Map.Entry<String, List<String>>> entries = commitInfo.getPartitionDirAndValsMap().entrySet();
            if (!CollectionUtils.isEmpty(entries)) {
                partitionDirAndValsMap.putAll(commitInfo.getPartitionDirAndValsMap());
            }
        });
        return new FileAggregatedCommitInfo(aggregateCommitInfo, partitionDirAndValsMap);
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfoList) throws Exception {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return;
        }
        aggregatedCommitInfoList.forEach(aggregateCommitInfo -> {
            try {
                fileSystemCommitter.abortTransaction(aggregateCommitInfo);
            } catch (Exception e) {
                LOGGER.error("abort aggregateCommitInfo error ", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
    }
}

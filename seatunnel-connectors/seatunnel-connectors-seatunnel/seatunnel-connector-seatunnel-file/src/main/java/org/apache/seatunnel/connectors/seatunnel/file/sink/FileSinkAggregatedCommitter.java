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
import org.apache.seatunnel.connectors.seatunnel.file.utils.HdfsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSinkAggregatedCommitter implements SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSinkAggregatedCommitter.class);

    @Override
    public List<FileAggregatedCommitInfo> commit(List<FileAggregatedCommitInfo> aggregatedCommitInfoList) throws IOException {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return null;
        }
        List errorAggregatedCommitInfoList = new ArrayList();
        aggregatedCommitInfoList.stream().forEach(aggregateCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getTransactionMap().entrySet()) {
                    // rollback the file
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        HdfsUtils.renameFile(mvFileEntry.getKey(), mvFileEntry.getValue(), true);
                    }
                    // delete the transaction dir
                    HdfsUtils.deleteFile(entry.getKey());
                }
            } catch (IOException e) {
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
        commitInfos.stream().forEach(commitInfo -> {
            Map<String, String> needMoveFileMap = aggregateCommitInfo.get(commitInfo.getTransactionDir());
            if (needMoveFileMap == null) {
                needMoveFileMap = new HashMap<>();
                aggregateCommitInfo.put(commitInfo.getTransactionDir(), needMoveFileMap);
            }
            needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
        });
        return new FileAggregatedCommitInfo(aggregateCommitInfo);
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfoList) throws Exception {
        if (aggregatedCommitInfoList == null || aggregatedCommitInfoList.size() == 0) {
            return;
        }
        aggregatedCommitInfoList.stream().forEach(aggregateCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getTransactionMap().entrySet()) {
                    // rollback the file
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        if (HdfsUtils.fileExist(mvFileEntry.getValue()) && !HdfsUtils.fileExist(mvFileEntry.getKey())) {
                            HdfsUtils.renameFile(mvFileEntry.getValue(), mvFileEntry.getKey(), true);
                        }
                    }
                    // delete the transaction dir
                    HdfsUtils.deleteFile(entry.getKey());
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

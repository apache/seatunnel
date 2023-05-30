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

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Deprecated interface since 2.3.0-beta, now used {@link FileSinkAggregatedCommitter} */
@Deprecated
public class FileSinkCommitter implements SinkCommitter<FileCommitInfo> {
    private final FileSystemUtils fileSystemUtils;

    public FileSinkCommitter(FileSystemUtils fileSystemUtils) {
        this.fileSystemUtils = fileSystemUtils;
    }

    @Override
    public List<FileCommitInfo> commit(List<FileCommitInfo> commitInfos) throws IOException {
        ArrayList<FileCommitInfo> failedCommitInfos = new ArrayList<>();
        for (FileCommitInfo commitInfo : commitInfos) {
            Map<String, String> needMoveFiles = commitInfo.getNeedMoveFiles();
            needMoveFiles.forEach(
                    (k, v) -> {
                        try {
                            fileSystemUtils.renameFile(k, v, true);
                        } catch (IOException e) {
                            failedCommitInfos.add(commitInfo);
                        }
                    });
            fileSystemUtils.deleteFile(commitInfo.getTransactionDir());
        }
        return failedCommitInfos;
    }

    /**
     * Abort the transaction, this method will be called (**Only** on Spark engine) when the commit
     * is failed.
     *
     * @param commitInfos The list of commit message, used to abort the commit.
     * @throws IOException throw IOException when close failed.
     */
    @Override
    public void abort(List<FileCommitInfo> commitInfos) throws IOException {
        for (FileCommitInfo commitInfo : commitInfos) {
            Map<String, String> needMoveFiles = commitInfo.getNeedMoveFiles();
            for (Map.Entry<String, String> entry : needMoveFiles.entrySet()) {
                if (fileSystemUtils.fileExist(entry.getValue())
                        && !fileSystemUtils.fileExist(entry.getKey())) {
                    fileSystemUtils.renameFile(entry.getValue(), entry.getKey(), true);
                }
            }
            fileSystemUtils.deleteFile(commitInfo.getTransactionDir());
        }
    }
}

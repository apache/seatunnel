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
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class FileSinkAggregatedCommitter
        implements SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo> {
    protected HadoopFileSystemProxy hadoopFileSystemProxy;
    private final HadoopConf hadoopConf;
    private final Set<String> pendingUuidDirectories = new LinkedHashSet<>();
    private final Set<String> pendingJobDirectories = new LinkedHashSet<>();

    public FileSinkAggregatedCommitter(HadoopConf hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public void init() {
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        List<FileAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        aggregatedCommitInfos.forEach(
                aggregatedCommitInfo -> {
                    try {
                        for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                                aggregatedCommitInfo.getTransactionMap().entrySet()) {
                            for (Map.Entry<String, String> mvFileEntry :
                                    entry.getValue().entrySet()) {
                                // first rename temp file
                                hadoopFileSystemProxy.renameFile(
                                        mvFileEntry.getKey(), mvFileEntry.getValue(), true);
                            }
                            String transactionDir = entry.getKey();
                            // Data files are already committed after rename; tmp cleanup is
                            // best-effort and should not fail the whole checkpoint.
                            try {
                                hadoopFileSystemProxy.deleteFile(transactionDir);
                                registerTransactionParentDirectories(transactionDir);
                            } catch (Exception cleanupException) {
                                log.warn(
                                        "delete transaction directory [{}] failed after successful commit, ignore this cleanup error.",
                                        transactionDir,
                                        cleanupException);
                            }
                        }
                    } catch (Throwable e) {
                        log.error(
                                "commit aggregatedCommitInfo error, aggregatedCommitInfo = {} ",
                                aggregatedCommitInfo,
                                e);
                        errorAggregatedCommitInfoList.add(aggregatedCommitInfo);
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
    public FileAggregatedCommitInfo combine(List<FileCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.size() == 0) {
            return null;
        }
        LinkedHashMap<String, LinkedHashMap<String, String>> aggregateCommitInfo =
                new LinkedHashMap<>();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>();
        commitInfos.forEach(
                commitInfo -> {
                    LinkedHashMap<String, String> needMoveFileMap =
                            aggregateCommitInfo.computeIfAbsent(
                                    commitInfo.getTransactionDir(), k -> new LinkedHashMap<>());
                    needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
                    if (commitInfo.getPartitionDirAndValuesMap() != null
                            && !commitInfo.getPartitionDirAndValuesMap().isEmpty()) {
                        partitionDirAndValuesMap.putAll(commitInfo.getPartitionDirAndValuesMap());
                    }
                });
        return new FileAggregatedCommitInfo(aggregateCommitInfo, partitionDirAndValuesMap);
    }

    /**
     * If {@link #commit(List)} failed, this method will be called (**Only** on Spark engine at
     * now).
     *
     * @param aggregatedCommitInfos The list of combine commit message.
     * @throws Exception throw Exception when abort failed.
     */
    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        log.info("rollback aggregate commit");
        if (aggregatedCommitInfos == null || aggregatedCommitInfos.size() == 0) {
            return;
        }
        aggregatedCommitInfos.forEach(
                aggregatedCommitInfo -> {
                    try {
                        for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                                aggregatedCommitInfo.getTransactionMap().entrySet()) {
                            // rollback the file
                            for (Map.Entry<String, String> mvFileEntry :
                                    entry.getValue().entrySet()) {
                                if (hadoopFileSystemProxy.fileExist(mvFileEntry.getValue())
                                        && !hadoopFileSystemProxy.fileExist(mvFileEntry.getKey())) {
                                    hadoopFileSystemProxy.renameFile(
                                            mvFileEntry.getValue(), mvFileEntry.getKey(), true);
                                }
                            }
                            // delete the transaction dir
                            String transactionDir = entry.getKey();
                            hadoopFileSystemProxy.deleteFile(transactionDir);
                            cleanupTransactionParentDirectories(transactionDir);
                        }
                    } catch (Exception e) {
                        log.error("abort aggregatedCommitInfo error ", e);
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
        try {
            cleanupPendingTransactionParentDirectories();
        } finally {
            hadoopFileSystemProxy.close();
        }
    }

    private void registerTransactionParentDirectories(String transactionDir) {
        Path uuidDir = new Path(transactionDir).getParent();
        if (uuidDir == null) {
            return;
        }
        Path jobDir = uuidDir.getParent();
        if (jobDir == null) {
            return;
        }
        pendingUuidDirectories.add(uuidDir.toString());
        pendingJobDirectories.add(jobDir.toString());
    }

    private void cleanupTransactionParentDirectories(String transactionDir) {
        Path uuidDir = new Path(transactionDir).getParent();
        if (uuidDir == null) {
            return;
        }
        Path jobDir = uuidDir.getParent();
        if (jobDir == null) {
            return;
        }
        cleanupEmptyDirectory(uuidDir.toString());
        cleanupEmptyDirectory(jobDir.toString());
    }

    private void cleanupPendingTransactionParentDirectories() {
        pendingUuidDirectories.forEach(this::cleanupEmptyDirectory);
        pendingJobDirectories.forEach(this::cleanupEmptyDirectory);
        pendingUuidDirectories.clear();
        pendingJobDirectories.clear();
    }

    private void cleanupEmptyDirectory(String directory) {
        try {
            hadoopFileSystemProxy.deleteEmptyDirectory(directory);
        } catch (IOException e) {
            log.warn("Failed to clean empty transaction parent directory: {}", directory, e);
        }
    }
}

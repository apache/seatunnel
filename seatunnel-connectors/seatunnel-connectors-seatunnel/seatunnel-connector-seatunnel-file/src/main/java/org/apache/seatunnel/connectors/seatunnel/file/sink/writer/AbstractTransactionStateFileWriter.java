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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.Constant;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.utils.HdfsUtils;

import com.google.common.collect.Lists;
import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractTransactionStateFileWriter implements TransactionStateFileWriter {
    protected Map<String, String> needMoveFiles;
    protected SeaTunnelRowType seaTunnelRowTypeInfo;
    protected String jobId;
    protected int subTaskIndex;

    protected Map<String, String> beingWrittenFile;

    protected String transactionId;

    protected String transactionDir;

    private long checkpointId;

    private TransactionFileNameGenerator transactionFileNameGenerator;

    protected List<Integer> sinkColumnsIndexInRow;

    private String targetPath;

    private String tmpPath;

    private PartitionDirNameGenerator partitionDirNameGenerator;

    public AbstractTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                              @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                              @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                              @NonNull List<Integer> sinkColumnsIndexInRow,
                                              @NonNull String tmpPath,
                                              @NonNull String targetPath,
                                              @NonNull String jobId,
                                              int subTaskIndex) {
        checkArgument(subTaskIndex > -1);

        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.transactionFileNameGenerator = transactionFileNameGenerator;
        this.sinkColumnsIndexInRow = sinkColumnsIndexInRow;
        this.tmpPath = tmpPath;
        this.targetPath = targetPath;
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.partitionDirNameGenerator = partitionDirNameGenerator;
    }

    public String getOrCreateFilePathBeingWritten(@NonNull SeaTunnelRow seaTunnelRow) {
        String beingWrittenFileKey = this.partitionDirNameGenerator.generatorPartitionDir(seaTunnelRow);
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            StringBuilder sbf = new StringBuilder(this.transactionDir);
            sbf.append("/")
                .append(beingWrittenFileKey)
                .append("/")
                .append(transactionFileNameGenerator.generateFileName(this.transactionId));
            String newBeingWrittenFilePath = sbf.toString();
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String seaTunnelFilePath) {
        String tmpPath = seaTunnelFilePath.replaceAll(this.transactionDir, targetPath);
        return tmpPath.replaceAll(Constant.NON_PARTITION + "/", "");
    }

    @Override
    public String beginTransaction(@NonNull Long checkpointId) {
        this.finishAndCloseWriteFile();
        this.transactionId = "T" + Constant.TRANSACTION_ID_SPLIT + jobId + Constant.TRANSACTION_ID_SPLIT + subTaskIndex + Constant.TRANSACTION_ID_SPLIT + checkpointId;
        this.transactionDir = getTransactionDir(this.transactionId);
        this.needMoveFiles = new HashMap<>();
        this.beingWrittenFile = new HashMap<>();
        this.beginTransaction(this.transactionId);
        this.checkpointId = checkpointId;
        return this.transactionId;
    }

    private String getTransactionDir(@NonNull String transactionId) {
        StringBuilder sbf = new StringBuilder(this.tmpPath);
        sbf.append("/")
            .append(Constant.SEATUNNEL)
            .append("/")
            .append(jobId)
            .append("/")
            .append(transactionId);
        return sbf.toString();
    }

    public abstract void beginTransaction(String transactionId);

    @Override
    public void abortTransaction() {
        this.finishAndCloseWriteFile();
        //drop transaction dir
        try {
            abortTransaction(this.transactionId);
            HdfsUtils.deleteFile(this.transactionDir);
        } catch (IOException e) {
            throw new RuntimeException("abort transaction " + this.transactionId + " error.", e);
        }
    }

    public abstract void abortTransaction(String transactionId);

    @Override
    public List<String> getTransactionAfter(@NonNull String transactionId) {
        StringBuilder sbf = new StringBuilder(this.targetPath);
        sbf.append("/")
            .append(Constant.SEATUNNEL)
            .append("/")
            .append(jobId)
            .append("/");
        String jobDir = sbf.toString();

        //get all transaction dir
        try {
            List<Path> transactionDirList = HdfsUtils.dirList(jobDir);
            List<String> transactionList = transactionDirList
                .stream()
                .map(dir -> dir.getName().replaceAll(jobDir, ""))
                .collect(Collectors.toList());
            return transactionList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        this.finishAndCloseWriteFile();
        // this.needMoveFiles will be clear when beginTransaction, so we need copy the needMoveFiles.
        Map<String, String> commitMap = new HashMap<>();
        commitMap.putAll(this.needMoveFiles);
        return Optional.of(new FileCommitInfo(commitMap, this.transactionDir));
    }

    @Override
    public void abortTransactions(List<String> transactionIds) {
        if (CollectionUtils.isEmpty(transactionIds)) {
            return;
        }

        transactionIds.stream().forEach(transactionId -> {
            try {
                abortTransaction(transactionId);
                HdfsUtils.deleteFile(transactionId);
            } catch (IOException e) {
                throw new RuntimeException("abort transaction " + transactionId + " error.", e);
            }
        });
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) {
        ArrayList<FileSinkState> fileSinkStates = Lists.newArrayList(new FileSinkState(this.transactionId, this.checkpointId));
        return fileSinkStates;
    }
}

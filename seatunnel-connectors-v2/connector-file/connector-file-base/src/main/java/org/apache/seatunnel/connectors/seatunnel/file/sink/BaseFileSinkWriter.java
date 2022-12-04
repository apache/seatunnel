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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BaseFileSinkWriter implements SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> {
    private final WriteStrategy writeStrategy;
    private final HadoopConf hadoopConf;
    private final SinkWriter.Context context;
    private final int subTaskIndex;
    private final String jobId;

    public BaseFileSinkWriter(WriteStrategy writeStrategy, HadoopConf hadoopConf, SinkWriter.Context context, String jobId, List<FileSinkState> fileSinkStates) {
        this.writeStrategy = writeStrategy;
        this.context = context;
        this.hadoopConf = hadoopConf;
        this.jobId = jobId;
        this.subTaskIndex = context.getIndexOfSubtask();
        writeStrategy.init(hadoopConf, jobId, subTaskIndex);
        if (!fileSinkStates.isEmpty()) {
            List<String> transactionIds = writeStrategy.getTransactionIdFromStates(fileSinkStates);
            transactionIds.forEach(writeStrategy::abortPrepare);
            writeStrategy.beginTransaction(fileSinkStates.get(0).getCheckpointId() + 1);
        } else {
            writeStrategy.beginTransaction(1L);
        }
    }

    public BaseFileSinkWriter(WriteStrategy writeStrategy, HadoopConf hadoopConf, SinkWriter.Context context, String jobId) {
        this(writeStrategy, hadoopConf, context, jobId, Collections.emptyList());
        writeStrategy.beginTransaction(1L);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            writeStrategy.write(element);
        } catch (FileConnectorException e) {
            String errorMsg = String.format("Write this data [%s] to file failed", element);
            throw new FileConnectorException(CommonErrorCode.FILE_OPERATION_FAILED, errorMsg, e);
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        return writeStrategy.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        writeStrategy.abortPrepare();
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) throws IOException {
        return writeStrategy.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
    }
}

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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.serialization.RowConverter;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

@Slf4j
public class SeaTunnelSparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    private final SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter;

    @Nullable private final SinkCommitter<CommitInfoT> sinkCommitter;
    private final RowConverter<InternalRow> rowConverter;
    private CommitInfoT latestCommitInfoT;
    private long epochId;
    private volatile MultiTableResourceManager resourceManager;

    public SeaTunnelSparkDataWriter(
            SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter,
            @Nullable SinkCommitter<CommitInfoT> sinkCommitter,
            SeaTunnelDataType<?> dataType,
            long epochId) {
        this.sinkWriter = sinkWriter;
        this.sinkCommitter = sinkCommitter;
        this.rowConverter = new InternalRowConverter(dataType);
        this.epochId = epochId == 0 ? 1 : epochId;
        initResourceManger();
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(rowConverter.reconvert(record));
    }

    private void initResourceManger() {
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        Optional<CommitInfoT> commitInfoTOptional = sinkWriter.prepareCommit();
        commitInfoTOptional.ifPresent(commitInfoT -> latestCommitInfoT = commitInfoT);
        sinkWriter.snapshotState(epochId++);
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.commit(Collections.emptyList());
            } else {
                sinkCommitter.commit(Collections.singletonList(latestCommitInfoT));
            }
        }
        SeaTunnelSparkWriterCommitMessage<CommitInfoT> seaTunnelSparkWriterCommitMessage =
                new SeaTunnelSparkWriterCommitMessage<>(latestCommitInfoT);
        cleanCommitInfo();
        sinkWriter.close();
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        return seaTunnelSparkWriterCommitMessage;
    }

    @Override
    public void abort() throws IOException {
        sinkWriter.abortPrepare();
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.abort(Collections.emptyList());
            } else {
                sinkCommitter.abort(Collections.singletonList(latestCommitInfoT));
            }
        }
        cleanCommitInfo();
    }

    private void cleanCommitInfo() {
        latestCommitInfoT = null;
    }

    @Override
    public void close() throws IOException {}
}

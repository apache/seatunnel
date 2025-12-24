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

package org.apache.seatunnel.translation.spark.sink.writer;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

@Slf4j
public class SparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    protected final SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter;

    @Nullable protected final SinkCommitter<CommitInfoT> sinkCommitter;
    protected CommitInfoT latestCommitInfoT;
    protected long epochId;
    protected volatile MultiTableResourceManager resourceManager;

    private final MultiTableManager multiTableManager;
    private final org.apache.seatunnel.api.sink.SinkWriter.Context context;

    SparkDataWriter(
            SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter,
            @Nullable SinkCommitter<CommitInfoT> sinkCommitter,
            MultiTableManager multiTableManager,
            long epochId,
            org.apache.seatunnel.api.sink.SinkWriter.Context context) {
        this.sinkWriter = sinkWriter;
        this.sinkCommitter = sinkCommitter;
        this.epochId = epochId == 0 ? 1 : epochId;
        this.multiTableManager = multiTableManager;
        this.context = context;
        initResourceManger();
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(multiTableManager.reconvert(record));
    }

    protected void initResourceManger() {
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // We combine the prepareCommit and commit in this method.
        // If this method fails, we need to rollback the transaction in the abort method.
        // 1. prepareCommit fails:
        //   1.1. We don't have the commit info, we need to execute the sinkWriter#abort to rollback
        // the transaction.
        // 2. commit fails
        //   2.1. We have the commit info, we need to execute the sinkCommitter#abort to rollback
        // the transaction.
        Optional<CommitInfoT> commitInfoTOptional = sinkWriter.prepareCommit(epochId);
        commitInfoTOptional.ifPresent(commitInfoT -> latestCommitInfoT = commitInfoT);
        sinkWriter.snapshotState(epochId++);
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.commit(Collections.emptyList());
            } else {
                sinkCommitter.commit(Collections.singletonList(latestCommitInfoT));
            }
        }
        SparkWriterCommitMessage<CommitInfoT> sparkWriterCommitMessage =
                new SparkWriterCommitMessage<>(latestCommitInfoT);
        cleanCommitInfo();
        sinkWriter.close();
        context.getEventListener().onEvent(new WriterCloseEvent());
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        return sparkWriterCommitMessage;
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
}

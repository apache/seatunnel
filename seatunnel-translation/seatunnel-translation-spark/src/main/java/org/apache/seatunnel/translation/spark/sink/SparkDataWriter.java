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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.serialization.RowSerialization;
import org.apache.seatunnel.translation.spark.serialization.InternalRowSerialization;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public class SparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    private SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter;

    @Nullable
    private SinkCommitter<CommitInfoT> sinkCommitter = null;
    private final RowSerialization<InternalRow> rowSerialization;
    private CommitInfoT latestCommitInfoT;
    private long epochId;

    SparkDataWriter(SinkWriter.Context context,
                    StructType schema, long epochId, String sinkString) {
        try {
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, ?> sink = SerializationUtils.stringToObject(sinkString);
            this.sinkWriter = sink.createWriter(context);
            Optional<SinkCommitter<CommitInfoT>> optionalSinkCommitter = sink.createCommitter();
            optionalSinkCommitter.ifPresent(commitInfoTSinkCommitter -> this.sinkCommitter = commitInfoTSinkCommitter);
        } catch (Exception e) {
            throw new RuntimeException("failed create SparkDataWriter", e);
        }
        this.rowSerialization = new InternalRowSerialization(schema);
        this.epochId = epochId == 0 ? 1 : epochId;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(rowSerialization.deserialize(record));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // We combine the prepareCommit and commit in this method.
        // If this method fails, we need to rollback the transaction in the abort method.
        // 1. prepareCommit fails:
        //   1.1. We don't have the commit info, we need to execute the sinkWriter#abort to rollback the transaction.
        // 2. commit fails
        //   2.1. We have the commit info, we need to execute the sinkCommitter#abort to rollback the transaction.
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
        SparkWriterCommitMessage<CommitInfoT> sparkWriterCommitMessage = new SparkWriterCommitMessage<>(latestCommitInfoT);
        cleanCommitInfo();
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

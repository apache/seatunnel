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

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.serialization.SparkRowSerialization;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;

public class SparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    private final SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter;

    @Nullable
    private final SinkCommitter<CommitInfoT> sinkCommitter;
    private final StructType schema;
    private final SparkRowSerialization rowSerialization = new SparkRowSerialization();

    SparkDataWriter(SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter,
                    SinkCommitter<CommitInfoT> sinkCommitter,
                    StructType schema) {
        this.sinkWriter = sinkWriter;
        this.sinkCommitter = sinkCommitter;
        this.schema = schema;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(rowSerialization.deserialize(schema, record));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        CommitInfoT commitInfo = sinkWriter.prepareCommit();
        if (sinkCommitter != null) {
            sinkCommitter.commit(Collections.singletonList(commitInfo));
        }
        return new SparkWriterCommitMessage<>(commitInfo);
    }

    @Override
    public void abort() throws IOException {
        if (sinkCommitter != null) {
            sinkCommitter.abort();
        }
    }
}
